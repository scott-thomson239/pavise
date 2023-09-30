package pavise

import scala.concurrent.duration.FiniteDuration

trait Metadata[F[_]]:
  def cluster: F[Cluster]
  def addTopic(topic: String): F[F[Cluster]]

object Metadata:
  def create[F[_]](
      metadataMaxAge: FiniteDuration,
      metadataMaxIdle: FiniteDuration,
      client: KafkaClient[F]
  ): F[Metadata[F]] = ???
