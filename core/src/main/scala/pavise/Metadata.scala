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
import fs2.concurrent.SignallingRef

trait Metadata[F[_]]:
  def currentLeader(topicPartition: TopicPartition): F[Int]

object Metadata:

  def resource[F[_]: Async](
      metadataMaxAge: FiniteDuration,
      metadataMaxIdle: FiniteDuration,
      clusterState: SignallingRef[F, ClusterState]
  ): Resource[F, Metadata[F]] =
    backgroundUpdater(metadataMaxAge, clusterState).map { _ =>
      new Metadata[F]:
        def currentLeader(topicPartition: TopicPartition): F[Int] =
          clusterState.get.flatMap { state =>
            state.cluster.partitionByTopicPartition
              .get(topicPartition)
              .fold {
                requestUpdateForTopics(clusterState, Set(topicPartition.topic)) *>
                  clusterState.waitUntil(
                    _.cluster.partitionsByTopic.get(topicPartition.topic).nonEmpty
                  ) *>
                  clusterState.get.flatMap(
                    _.cluster.partitionByTopicPartition
                      .get(topicPartition)
                      .map(_.leader.id)
                      .liftTo[F](new Exception("cant find leader"))
                  )
              }(_.leader.id.pure[F])
          }
    }
  private def backgroundUpdater[F[_]: Async](
      metadataMaxAge: FiniteDuration,
      clusterState: SignallingRef[F, ClusterState]
  ): Resource[F, Fiber[F, Throwable, Nothing]] = Resource.make {
    Async[F].sleep(metadataMaxAge) *> clusterState.get
      .flatMap { state =>
        requestUpdateForTopics(clusterState, state.cluster.partitionsByTopic.keySet)
      }
      .foreverM
      .start
  }(_.cancel)

  private def requestUpdateForTopics[F[_]](
      clusterState: SignallingRef[F, ClusterState],
      topics: Set[String]
  ): F[Unit] = clusterState.update { state =>
    ClusterState(state.waitingTopics.union(topics), state.cluster)
  }
