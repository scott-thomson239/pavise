package pavise.producer

import scala.concurrent.duration.FiniteDuration
import pavise.TopicPartition

case class RecordMetadata(
    offset: Long,
    timestamp: FiniteDuration,
    serializedKeySize: Int,
    serializedValueSize: Int,
    topicPartition: TopicPartition
)
