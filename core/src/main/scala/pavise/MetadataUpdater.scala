package pavise

import cats.syntax.all.*
import pavise.Cluster.*
import fs2.concurrent.SignallingRef
import fs2.*
import cats.effect.kernel.Ref
import cats.effect.kernel.Async
import pavise.protocol.message.MetadataRequest
import pavise.protocol.message.MetadataResponse
import com.comcast.ip4s.SocketAddress
import com.comcast.ip4s.Host
import com.comcast.ip4s.Port

object MetadataUpdater:
  def apply[F[_]: Async](
      client: KafkaClient[F],
      clusterState: SignallingRef[F, ClusterState]
  ): F[Unit] =
    Stream
      .eval(Ref.of(Set.empty[String]))
      .flatMap { inProgress =>
        clusterState.discrete
          .evalMap { state =>
            inProgress.modify { curInProgress =>
              if state.waitingTopics.diff(curInProgress).isEmpty then
                (curInProgress, Set.empty[String])
              else
                val union = curInProgress.union(state.waitingTopics)
                (union, union)
            }
          }
          .evalMap { topics =>
            val request = MetadataRequest(topics.toList, false, false, false)
            sendMetadataRequest(client, request).flatMap { newCluster =>
              clusterState.update { prevState =>
                ClusterState(topics.diff(prevState.waitingTopics), newCluster)
              }
            }
          }
      }
      .spawn
      .compile
      .drain

  private def sendMetadataRequest[F[_]: Async](
      client: KafkaClient[F],
      request: MetadataRequest
  ): F[Cluster] =
    client.leastUsedNode.flatMap { node =>
      client.sendRequest(node, request).flatten.flatMap { resp =>
        clusterFromResponse(resp).liftTo[F](new Exception("bad response"))
      }
    }

  // TODO: probably shouldn't reconstruct entire cluster each update
  private def clusterFromResponse(response: MetadataResponse): Option[Cluster] = for
    nodes <- response.brokers.traverse { b =>
      (Host.fromString(b.host), Port.fromInt(b.port)).mapN { (host, port) =>
        Node(b.nodeId, host, port)
      }
    }
    controller <- nodes.find(_.id == response.controllerId)
    partitionInfos <- response.topics.flatTraverse { topicMetadata =>
      topicMetadata.partitions.traverse { partition =>
        (
          nodes.find(_.id == partition.leaderId),
          partition.replicaNodes.traverse(rep => nodes.find(_.id == rep)),
          partition.isrNodes.traverse(isr => nodes.find(_.id == isr)),
          partition.offlineReplicas.traverse(off => nodes.find(_.id == off))
        ).mapN { (leader, replicas, inSync, offline) =>
          PartitionInfo(
            topicMetadata.name,
            partition.partitionIndex,
            leader,
            replicas,
            inSync,
            offline
          )
        }
      }
    }
    partitionByTopicPartition = partitionInfos.groupBy(part =>
      TopicPartition(part.topic, part.partition)
    ).mapValues(_.head).toMap
    nodeMap = nodes.groupBy(_.id).mapValues(_.head).toMap
    partitionsByTopic = partitionInfos.groupBy(part => part.topic)
    partitionsByNode = partitionInfos.groupBy(part => part.leader.id)
  yield Cluster(
    nodeMap,
    Set.empty,
    Set.empty,
    Set.empty,
    Some(controller),
    partitionByTopicPartition,
    partitionsByTopic,
    partitionsByNode
  )
