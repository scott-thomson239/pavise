package pavise

import scala.concurrent.duration.FiniteDuration
import cats.syntax.all.*
import cats.effect.syntax.all.*
import fs2.*
import cats.effect.kernel.Ref
import cats.effect.kernel.Temporal
import pavise.protocol.message.*
import cats.effect.kernel.Resource
import cats.effect.kernel.Fiber
import fs2.concurrent.SignallingRef
import com.comcast.ip4s.SocketAddress
import com.comcast.ip4s.Host

trait Metadata[F[_]]:
  def currentLeader(topicPartition: TopicPartition): F[Int]
  def allNodes: F[Map[Int, Node]]

object Metadata:

  def resource[F[_]: Temporal](
      bootstrapServers: List[SocketAddress[Host]],
      metadataMaxAge: FiniteDuration,
      metadataMaxIdle: FiniteDuration,
      clusterState: SignallingRef[F, ClusterState]
  ): Resource[F, Metadata[F]] =
    (
      backgroundUpdater(metadataMaxAge, clusterState),
      Resource.eval(
        clusterState.update(state =>
          state.copy(cluster = state.cluster.copy(nodes = addressesToNodes(bootstrapServers)))
        )
      )
    ).mapN { (_, _) =>
      new Metadata[F]:
        def currentLeader(topicPartition: TopicPartition): F[Int] =
          clusterState.get.flatMap { state =>
            state.cluster.partitionByTopicPartition
              .get(topicPartition)
              .fold {
                requestUpdateForTopics(clusterState, Set(topicPartition.topic)) *>
                  clusterState.waitUntil(
                    !_.waitingTopics.contains(topicPartition.topic)
                  ) *>
                  clusterState.get.flatMap(
                    _.cluster.partitionByTopicPartition
                      .get(topicPartition)
                      .map(_.leader.id)
                      .liftTo[F](new Exception("cant find leader"))
                  )
              }(_.leader.id.pure[F])
          }
        def allNodes: F[Map[Int, Node]] =
          clusterState.get.map(_.cluster.nodes)
    }

  private def addressesToNodes(addrs: List[SocketAddress[Host]]): Map[Int, Node] =
    addrs.mapWithIndex((addr, i) => (-i, Node(-i, addr.host, addr.port))).toMap

  private def backgroundUpdater[F[_]: Temporal](
      metadataMaxAge: FiniteDuration,
      clusterState: SignallingRef[F, ClusterState]
  ): Resource[F, Fiber[F, Throwable, Nothing]] = Resource.make {
    Temporal[F].sleep(metadataMaxAge) *> clusterState.get
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
