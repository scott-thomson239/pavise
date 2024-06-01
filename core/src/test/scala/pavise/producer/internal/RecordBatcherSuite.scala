package pavise

import cats.effect.IO
import munit.CatsEffectSuite
import pavise.producer.internal.RecordBatcher
import cats.effect.kernel.Resource
import scala.concurrent.duration.*
import pavise.producer.internal.BatchSender
import pavise.protocol.RecordBatch
import pavise.producer.RecordMetadata
import scodec.bits.ByteVector
import cats.effect.kernel.Async
import pavise.producer.ProducerRecord
import cats.effect.kernel.Ref
import cats.effect.kernel.Deferred
import cats.syntax.all.*
import cats.effect.testkit.TestControl
import cats.effect.kernel.Outcome

class RecordBatcherSuite extends CatsEffectSuite:

  val BATCH_SIZE = 400

  def recordBatcherResource(
      batchSize: Int,
      linger: FiniteDuration,
      batchSender: BatchSender[IO]
  ): Resource[IO, RecordBatcher[IO, String, ByteVector]] =
    RecordBatcher.resource[IO, String, ByteVector](
      batchSize,
      CompressionType.None,
      linger,
      0.seconds,
      0,
      batchSender
    )(using
      Async[IO],
      Serializer.string[IO],
      Serializer.bytes[IO]
    )

  def testSender: IO[BatchSender[IO]] =
    Ref.of[IO, Map[TopicPartition, Long]](Map.empty).map { offsetMapRef =>
      new BatchSender[IO]:
        def send(topicPartition: TopicPartition, batch: RecordBatch): IO[IO[RecordMetadata]] =
          offsetMapRef
            .updateAndGet { offsetMap =>
              offsetMap.updatedWith(topicPartition)(prev => Some(prev.getOrElse(0.toLong) + 1))
            }
            .map { offsetMap =>
              IO(RecordMetadata(offsetMap(topicPartition), 0.seconds, 0, 0, topicPartition))
            }
    }

  def defSender(ref: Deferred[IO, RecordBatch]): BatchSender[IO] = new BatchSender[IO]:
    def send(topicPartition: TopicPartition, batch: RecordBatch): IO[IO[RecordMetadata]] =
      ref.complete(batch) *> IO(IO(RecordMetadata(0, 0.seconds, 0, 0, topicPartition)))

  def refSender(ref: Ref[IO, Option[TopicPartition]]): BatchSender[IO] = new BatchSender[IO]:
    def send(topicPartition: TopicPartition, batch: RecordBatch): IO[IO[RecordMetadata]] =
      ref.set(Some(topicPartition)) *> IO(
        IO(RecordMetadata(0, 0.seconds, 0, 0, topicPartition))
      )

  test("Adding a record to a batch that would equal batchSize causes that batch to be sent") {
    val TEST_PARTITION = 1
    Deferred[IO, RecordBatch].flatMap { firstBatchDef =>
      recordBatcherResource(BATCH_SIZE, 99.seconds, defSender(firstBatchDef)).use {
        recordBatcher =>
          val records = (1 to 4).map { index =>
            ProducerRecord("test", index.toString, ByteVector.low(BATCH_SIZE / 4), List.empty)
          }.toList

          val (head, rest) = (records.head, records.tail)
          rest.traverse_(rec => recordBatcher.batch(rec, TEST_PARTITION)) *>
            firstBatchDef.tryGet.assertEquals(None) *>
            recordBatcher.batch(head, TEST_PARTITION) *>
            firstBatchDef.get.assert(batch => batch.records.size == 3)
      }
    }
  }

  test("A batch that has not reached batchSize will be sent after linger has been reached") {
    val TEST_PARTITION = 1
    Deferred[IO, RecordBatch].flatMap { firstBatchDef =>
      recordBatcherResource(BATCH_SIZE, 30.seconds, defSender(firstBatchDef)).use {
        recordBatcher =>
          val record = ProducerRecord("test", "test", ByteVector.low(100), List.empty)
          TestControl.execute(recordBatcher.batch(record, TEST_PARTITION)).flatMap { control =>
            for
              _ <- control.tick
              _ <- firstBatchDef.tryGet.assertEquals(None)
              nextInterval <- control.nextInterval
              _ <- control.advanceAndTick(nextInterval)
              _ <- firstBatchDef.get.assert(batch => batch.records.size == 1)
              _ <- control.tickAll
              results <- control.results
              _ <- results match
                case Some(Outcome.Succeeded(res)) => IO.unit
                case _ => IO.raiseError(new Exception("outcome was not Succeeded"))
            yield ()
          }
      }
    }
  }

  test("Separate partition batches are sent independently of each other") {
    Ref.of[IO, Option[TopicPartition]](None).flatMap { lastPartitionRef =>
      recordBatcherResource(BATCH_SIZE, 30.seconds, refSender(lastPartitionRef)).use {
        recordBatcher =>
          val part1Records =
            (0 to 4)
              .map(i => ProducerRecord("test", s"part1$i", ByteVector.low(100), List.empty))
          val part2Records =
            (0 to 4)
              .map(i => ProducerRecord("test", s"part2$i", ByteVector.low(50), List.empty))
          TestControl
            .execute {
              part1Records.zip(part2Records).toList.traverse { case (r1, r2) =>
                (recordBatcher.batch(r1, 1), recordBatcher.batch(r2, 2)).tupled
              }
            }
            .flatMap { control =>
              for
                _ <- lastPartitionRef.get.assertEquals(None)
                _ <- control.tick
                _ <- lastPartitionRef.get.assertEquals(Some(TopicPartition("test", 1)))
                interval <- control.nextInterval
                _ <- control.advanceAndTick(interval)
                _ <- lastPartitionRef.get.assertEquals(Some(TopicPartition("test", 2)))
              yield ()
            }
      }
    }
  }

  test("Each RecordMetadata returned corresponds to correct record sent in batch call") {
    val TEST_PARTITION = 1
    testSender.flatMap { testSender =>
      recordBatcherResource(BATCH_SIZE, 30.seconds, testSender).use { recordBatcher =>
        val records =
          (0 to 1000).map(i =>
            ProducerRecord("test", i.toString, ByteVector.low(10), List.empty)
          )
        TestControl
          .executeEmbed {
            records.toList.traverse(r => recordBatcher.batch(r, TEST_PARTITION).flatten)
          }
          .map { metadataList =>
            metadataList.zipWithIndex.map { case (rm, i) =>
              assertEquals(rm.offset, i.toLong)
            }
          }
      }
    }
  }
