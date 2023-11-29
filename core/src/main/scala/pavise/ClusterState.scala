package pavise

import cats.effect.kernel.Concurrent
import fs2.concurrent.SignallingRef

case class ClusterState(waitingTopics: Set[String], cluster: Cluster)

object ClusterState:
  def apply[F[_]: Concurrent]: F[SignallingRef[F, ClusterState]] =
    SignallingRef(ClusterState(Set.empty, Cluster.empty))
