package pavise.producer.internal

import cats.effect.kernel.syntax.all.*
import cats.syntax.all.*
import pavise.producer.*
import cats.effect.kernel.Resource
import scodec.bits.ByteVector
import pavise.*
import cats.effect.std.MapRef
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
      MapRef.inConcurrentHashMap[F, F, TopicPartition, ByteVector]()
    )
    senderMap <- Resource.eval(
      MapRef.inConcurrentHashMap[F, F, TopicPartition, SenderFiber[F]]()
    )
  yield new RecordBatcher[F, K, V]:
    def batch(producerRecord: ProducerRecord[K, V], partition: Int): F[F[RecordMetadata]] = for
      recordBytes <- ByteVector.empty.pure[F] ////////////////////////////////////////////////////////
      topicPartition = TopicPartition(producerRecord.topic, partition)
      curBatch <- recordBatchMap(topicPartition).getAndUpdate {
        case Some(curBatch) if recordBytes.size + curBatch.size <= batchSize =>
          Some(curBatch ++ recordBytes)
        case _ =>
          Some(recordBytes.bufferBy(batchSize))
      }
      batchDef <- curBatch match
        case Some(batch) =>
          for
            sender <- senderMap(topicPartition).get.map(_.get) ////////////
            _ <-
              if batch.size + recordBytes.size > batchSize then sender.batchQueue.offer(batch)
              else Async[F].unit
            d <- sender.defQueue.take
            _ <- sender.defQueue.offer(d) ///////////////////////
          yield d
        case None =>
          for
            sender <- SenderFiber(
              batchSize,
              linger,
              topicPartition,
              batchSender,
              recordBatchMap
            )
            newDef <- Deferred[F, F[RecordMetadata]]
            _ <- sender.defQueue.offer(newDef)
            _ <- senderMap(topicPartition).set(Some(sender))
          yield newDef
      recordMetadata <- batchDef.get
    yield recordMetadata

  case class SenderFiber[F[_]](
      defQueue: Queue[F, Deferred[F, F[RecordMetadata]]],
      batchQueue: Queue[F, ByteVector],
      senderFiber: Fiber[F, Throwable, Unit]
  )

  object SenderFiber: // TODO: only start timer once batch is in progress
    def apply[F[_]: Async](
        batchSize: Int,
        linger: FiniteDuration,
        topicPartition: TopicPartition,
        batchSender: BatchSender[F],
        batchMap: MapRef[F, TopicPartition, Option[ByteVector]]
    ): F[SenderFiber[F]] = for
      defQueue <- Queue.synchronous[F, Deferred[F, F[RecordMetadata]]]
      batchQueue <- Queue.synchronous[F, ByteVector]
      takeIncomplete = batchMap(topicPartition).getAndSet(None)
      senderFiber <- batchQueue.take
        .map(a => Some(a))
        .timeoutTo(linger, takeIncomplete)
        .flatMap { batchOpt =>
          batchOpt.traverse_ { batch =>
            defQueue.take.flatMap { produceRes =>
              batchSender.send(topicPartition, batch).flatMap { metadataF =>
                produceRes.complete(metadataF)
              }
            }
          }
        }
        .foreverM
        .start
    yield SenderFiber(defQueue, batchQueue, senderFiber)
