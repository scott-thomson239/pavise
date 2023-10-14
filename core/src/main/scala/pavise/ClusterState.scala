package pavise

import cats.effect.kernel.Async
import fs2.concurrent.SignallingRef

case class ClusterState(waitingTopics: Set[String], cluster: Cluster)

object ClusterState:
  def apply[F[_]: Async]: F[SignallingRef[F, ClusterState]] =
    SignallingRef(ClusterState(Set.empty, Cluster.empty))
