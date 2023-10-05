package pavise

import scala.concurrent.duration.FiniteDuration

trait Metadata[F[_]]:
  def currentLeader(topicPartition: TopicPartition): F[Node]

object Metadata:
  def create[F[_]](
      metadataMaxAge: FiniteDuration,
      metadataMaxIdle: FiniteDuration,
      client: KafkaClient[F]
  ): F[Metadata[F]] = ???
