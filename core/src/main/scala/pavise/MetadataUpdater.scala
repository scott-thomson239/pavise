package pavise

import cats.syntax.all.*
import pavise.Cluster.*
import fs2.concurrent.SignallingRef
import fs2.*
import cats.effect.kernel.Ref
import cats.effect.kernel.Async
import pavise.protocol.message.MetadataRequest
import pavise.protocol.message.MetadataResponse

object MetadataUpdater:
  def apply[F[_]: Async](
      client: KafkaClient[F],
      clusterState: SignallingRef[F, ClusterState]
  ): F[Unit] =
    Stream
      .eval(Ref.of(Set.empty[String]))
      .flatMap { inProgress =>
        clusterState.discrete.evalMap { state =>
          inProgress.modify { curInProgress =>
            if state.waitingTopics.diff(curInProgress).isEmpty then
              (curInProgress, Set.empty[String])
            else {
              val union = curInProgress.union(state.waitingTopics)
              (union, union)
            }
          }
          }.evalMap { topics => 
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
      client.sendRequest(node, request).flatten.map { resp =>
        clusterFromResponse(resp)
      }
    }

  private def clusterFromResponse(response: MetadataResponse): Cluster = ???
