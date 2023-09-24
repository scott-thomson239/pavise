package pavise.producer.internal

import pavise.producer.RecordMetadata
import pavise.producer.ProducerRecord
import scala.concurrent.duration.FiniteDuration
import pavise.CompressionType
import pavise.KafkaClient

trait RecordAccumulator[F[_], K, V]:
  def send(producerRecord: ProducerRecord[K, V], partition: Int): F[F[RecordMetadata]]

object RecordAccumulator:

  def create[F[_], K, V](
    batchSize: Int,
    compressionType: CompressionType,
    linger: FiniteDuration,
    retryBackoff: FiniteDuration,
    deliveryTimeout: FiniteDuration,
    maxBlockTime: FiniteDuration,
    bufferMemory: Int,
    client: KafkaClient[F],
  ): F[RecordAccumulator[F, K, V]] = ???
