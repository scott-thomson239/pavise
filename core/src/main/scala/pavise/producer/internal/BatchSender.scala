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
import pavise.StreamMap
import cats.effect.kernel.Deferred
import cats.effect.kernel.Async
import pavise.protocol.message.ProduceResponse

trait BatchSender[F[_]]:
  def send(topicPartition: TopicPartition, batch: ByteVector): F[F[RecordMetadata]]

object BatchSender:
  def resource[F[_]: Async](
      retryBackoff: FiniteDuration,
      deliveryTimeout: FiniteDuration,
      maxBlockTime: FiniteDuration,
      metadata: Metadata[F],
      client: KafkaClient[F]
  ): Resource[F, BatchSender[F]] =
    KeyedResultStream.resource[F, Int, (TopicPartition, ByteVector), ProduceResponse].map { streamMap =>
      new BatchSender[F]:
        def send(topicPartition: TopicPartition, batch: ByteVector): F[F[RecordMetadata]] =
          metadata.currentLeader(topicPartition).flatMap { leader => 
            
          }

    }
