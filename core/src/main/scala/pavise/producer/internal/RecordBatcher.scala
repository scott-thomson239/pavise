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
import pavise.protocol.Record
import pavise.protocol.RecordBatch
import scala.concurrent.duration.*

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
  )(using
      keySerializer: KeySerializer[F, K],
      valueSerializer: ValueSerializer[F, V]
  ): Resource[F, RecordBatcher[F, K, V]] = for
    recordBatchMap <- Resource.eval(
      MapRef.inConcurrentHashMap[F, F, TopicPartition, (List[Record], Long)]()
    )
    senderMap <- Resource.eval(
      MapRef.inConcurrentHashMap[F, F, TopicPartition, SenderFiber[F]]()
    )
  yield new RecordBatcher[F, K, V]:
    def batch(producerRecord: ProducerRecord[K, V], partition: Int): F[F[RecordMetadata]] = for
      valueBytes <- valueSerializer.serialize(
        producerRecord.topic,
        producerRecord.headers,
        producerRecord.value
      )
      keyBytes <- keySerializer.serialize(
        producerRecord.topic,
        producerRecord.headers,
        producerRecord.key
      )
      record = createRecord(valueBytes, keyBytes, producerRecord.headers)
      topicPartition = TopicPartition(producerRecord.topic, partition)
      curBatch <- recordBatchMap(topicPartition).getAndUpdate {
        case Some((curBatch, curSize)) if valueBytes.size + curSize <= batchSize =>
          Some(record :: curBatch, curSize + valueBytes.size)
        case _ =>
          Some((List.empty[Record], 0))
      }
      batchDef <- curBatch match
        case Some((batch, size)) =>
          for
            sender <- senderMap(topicPartition).get.map(_.get) ////////////
            _ <-
              if size + valueBytes.size > batchSize then sender.batchQueue.offer(batch)
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
              compressionType,
              recordBatchMap
            )
            newDef <- Deferred[F, F[RecordMetadata]]
            _ <- sender.defQueue.offer(newDef)
            _ <- senderMap(topicPartition).set(Some(sender))
          yield newDef
      recordMetadata <- batchDef.get
    yield recordMetadata

  private def createRecord(key: ByteVector, value: ByteVector, headers: List[Header]): Record =
    Record(0, 0.seconds, 0, key, value, headers)

  private def createPartitionBatch(
      records: List[Record],
      topicPartition: TopicPartition,
      comp: CompressionType,
      producerId: Long
  ): RecordBatch =
    RecordBatch(
      0,
      0,
      0,
      0,
      0,
      RecordBatch.Attributes(
        comp,
        false,
        false,
        false,
        false
      ),
      0,
      0.seconds,
      0.seconds,
      0,
      0,
      0,
      records
    )

  case class SenderFiber[F[_]](
      defQueue: Queue[F, Deferred[F, F[RecordMetadata]]],
      batchQueue: Queue[F, List[Record]],
      senderFiber: Fiber[F, Throwable, Unit]
  )

  object SenderFiber: // TODO: only start timer once batch is in progress
    def apply[F[_]: Async](
        batchSize: Int,
        linger: FiniteDuration,
        topicPartition: TopicPartition,
        batchSender: BatchSender[F],
        compressionType: CompressionType,
        batchMap: MapRef[F, TopicPartition, Option[(List[Record], Long)]]
    ): F[SenderFiber[F]] = for
      defQueue <- Queue.synchronous[F, Deferred[F, F[RecordMetadata]]]
      batchQueue <- Queue.synchronous[F, List[Record]]
      takeIncomplete = batchMap(topicPartition).getAndSet(None).map(_._1F)
      senderFiber <- batchQueue.take
        .map(a => Some(a))
        .timeoutTo(linger, takeIncomplete)
        .flatMap { batchOpt =>
          batchOpt.traverse_ { batch =>
            val recordBatch = createPartitionBatch(batch, topicPartition, compressionType, 0)
            defQueue.take.flatMap { produceRes =>
              batchSender.send(topicPartition, recordBatch).flatMap { metadataF =>
                produceRes.complete(metadataF)
              }
            }
          }
        }
        .foreverM
        .start
    yield SenderFiber(defQueue, batchQueue, senderFiber)
