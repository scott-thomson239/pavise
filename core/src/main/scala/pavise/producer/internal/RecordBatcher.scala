package pavise.producer.internal

import cats.effect.kernel.syntax.all.*
import cats.syntax.all.*
import pavise.producer.*
import cats.effect.kernel.Resource
import scodec.bits.ByteVector
import pavise.*
import cats.effect.std.MapRef
import cats.effect.kernel.Sync
import scala.concurrent.duration.FiniteDuration
import cats.effect.kernel.Deferred
import cats.effect.kernel.Fiber
import cats.effect.std.Queue
import cats.effect.kernel.Async

trait RecordBatcher[F[_], K, V]:
  def batch(producerRecord: ProducerRecord[K, V], partition: Int): F[F[RecordMetadata]]

object RecordBatcher:
  def resource[F[_]: Async, K, V](
      batchSize: Int,
      compressionType: CompressionType,
      linger: FiniteDuration,
      maxBlockTime: FiniteDuration,
      bufferMemory: Int,
      batchSender: BatchSender[F]
  ): Resource[F, RecordBatcher[F, K, V]] = for
    recordBatchMap <- Resource.eval(
      MapRef.inConcurrentHashMap[F, F, TopicPartition, PartitionBatch[F]]()
    )
    senderMap <- Resource.eval(MapRef.inConcurrentHashMap[F, F, TopicPartition, SenderFiber[F]]())
  yield new RecordBatcher[F, K, V]:
    def batch(producerRecord: ProducerRecord[K, V], partition: Int): F[F[RecordMetadata]] = for {
      recordBytes <- ByteVector.empty.pure[F] ////////////////////////////////////////////////////////
      topicPartition = TopicPartition(producerRecord.topic, partition)
      batchRef = recordBatchMap(topicPartition)
      resDef <- batchRef.get.flatMap {
        case Some(b) =>
          if (recordBytes.size + b.bytes.size > batchSize) {
            Deferred[F, F[RecordMetadata]].flatMap { d => 
              batchRef.modify {
                case None => {
                  val newBatch = PartitionBatch(recordBytes.bufferBy(batchSize))
                  (Some(PartitionBatch(recordBytes.bufferBy(batchSize), d)))
                }
                case Some(a) => (Some(a), a)
              }.map(_.produceRes)
            }
          } else {

          }
        case None => ??? 
      }
    } yield resDef.get

  
  case class PartitionBatch[F[_]](bytes: ByteVector, produceRes: Deferred[F, F[RecordMetadata]])
  
  case class SenderFiber[F[_]](
      queue: Queue[F, PartitionBatch[F]],
      senderFiber: Fiber[F, Throwable, Unit]
  )

  object SenderFiber: // TODO: only start timer once batch is in progress
    def apply[F[_]: Async](
        batchSize: Int,
        linger: FiniteDuration,
        topicPartition: TopicPartition,
        batchSender: BatchSender[F],
        batchMap: MapRef[F, TopicPartition, Option[PartitionBatch[F]]]
    ): F[SenderFiber[F]] = for
      queue <- Queue.synchronous[F, PartitionBatch[F]]
      takeIncomplete = batchMap(topicPartition).getAndSet(None)
      senderFiber <- queue.take
        .map(a => Some(a))
        .timeoutTo(linger, takeIncomplete)
        .flatMap { batchOpt =>
          batchOpt.traverse_ { batch =>
            batchSender.send(topicPartition, batch.bytes).flatMap { metadataF =>
              batch.produceRes.complete(metadataF)
            }
          }
        }
        .foreverM
        .start
    yield SenderFiber(queue, senderFiber)
