package pavise.producer.internal

import pavise.producer.RecordMetadata
import pavise.producer.ProducerRecord
import scala.concurrent.duration.FiniteDuration
import pavise.CompressionType
import pavise.KafkaClient
import cats.effect.kernel.Resource
import cats.effect.std.MapRef
import cats.effect.kernel.Sync
import cats.syntax.all.*
import pavise.TopicPartition
import scodec.bits.*
import pavise.Metadata

trait BatchSender[F[_]]:
  def send(topicPartition: TopicPartition, batch: ByteVector): F[F[RecordMetadata]]

object BatchSender:
  def resource[F[_]: Sync](
    retryBackoff: FiniteDuration,
    deliveryTimeout: FiniteDuration,
    maxBlockTime: FiniteDuration,
    metadata: Metadata[F],
    client: KafkaClient[F],
  ): Resource[F, BatchSender[F]] = for {
    recordBatchMap <- Resource.eval(MapRef.inConcurrentHashMap[F, F, TopicPartition, ByteVector]())
  } yield
    new BatchSender[F]:
      def send(topicPartition: TopicPartition, batch: ByteVector): F[F[RecordMetadata]] = ???
