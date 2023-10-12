package pavise

import scala.concurrent.duration.FiniteDuration
import cats.syntax.all.*
import cats.effect.syntax.all.*
import fs2.*
import cats.effect.kernel.Ref
import cats.effect.kernel.Async
import pavise.protocol.message.*
import cats.effect.kernel.Resource
import cats.effect.kernel.Fiber

trait Metadata[F[_]]:
  def currentLeader(topicPartition: TopicPartition): F[Node]

object Metadata:
  def resource[F[_]: Async](
      metadataMaxAge: FiniteDuration,
      metadataMaxIdle: FiniteDuration,
      client: KafkaClient[F]
  ): Resource[F, Metadata[F]] = for
    clusterRef <- Resource.eval(Ref.of[F, Cluster](Cluster.empty))
    _ <- Resource.make(backgroundUpdater(metadataMaxAge, client, clusterRef))(_.cancel)
  yield new Metadata[F]:
    def currentLeader(topicPartition: TopicPartition): F[Node] =
      clusterRef.get.flatMap { cluster =>
        cluster.partitionByTopicPartition
          .get(topicPartition)
          .fold {
            val request = MetadataRequest(List(topicPartition.topic), false, false, false)
            sendMetadataRequest(client, clusterRef, request).flatMap { cluster =>
              cluster.partitionByTopicPartition
                .get(topicPartition)
                .map(_.leader)
                .liftTo[F](new Exception("cant find leader"))
            }
          }(_.leader.pure[F])
      }

  private def backgroundUpdater[F[_]: Async]( // use cur cluster topics, if none skip
      metadataMaxAge: FiniteDuration,
      client: KafkaClient[F],
      clusterRef: Ref[F, Cluster]
  ): F[Fiber[F, Throwable, Nothing]] =
    Async[F].sleep(metadataMaxAge) *> sendMetadataRequest(
      client,
      clusterRef,
      MetadataRequest(List(), false, false, false)
    ).foreverM.start

  private def sendMetadataRequest[F[_]: Async](
      client: KafkaClient[F],
      clusterRef: Ref[F, Cluster],
      request: MetadataRequest
  ): F[Cluster] =
    client.leastUsedNode.flatMap { node =>
      client.sendRequest(node, request).flatten.flatMap { resp =>
        clusterRef.updateAndGet { cluster =>
          updateCluster(resp, cluster)
        }
      }
    }

  private def updateCluster(response: MetadataResponse, curCluster: Cluster): Cluster = ???
