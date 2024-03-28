package pavise.producer.internal

import munit.CatsEffectSuite
import scala.concurrent.duration.*
import BatchSenderSuite.*
import pavise.*
import cats.effect.IO
import pavise.protocol.*
import cats.syntax.all.*
import cats.effect.kernel.Resource
import cats.effect.kernel.Ref
import pavise.protocol.message.*
import com.comcast.ip4s.*
import cats.effect.testkit.TestControl
import cats.effect.kernel.Outcome
import pavise.producer.RecordMetadata

class BatchSenderSuite extends CatsEffectSuite:

  test(
    "Batches are sent to the node that is the current leader of that partition"
  ) {
    def testKafkaClient(nodeList: Ref[IO, Set[Int]]): KafkaClient[IO] =
      new KafkaClient[IO]:
        def leastUsedNode: IO[Int] = 0.pure[IO]
        def sendRequest(nodeId: Int, request: KafkaRequest): IO[IO[request.RespT]] =
          nodeList.getAndUpdate(nodes => nodes + nodeId) *>
            IO(IO(ProduceResponse(List.empty, 0.seconds).asInstanceOf[request.RespT]))
    for
      nodesCalled <- Ref.of[IO, Set[Int]](Set.empty)
      kafkaClient = testKafkaClient(nodesCalled)
      _ <- BatchSender
        .resource(1, 50.seconds, 50.seconds, 50.seconds, testMetadata, kafkaClient)
        .use { batchSender =>
          (
            batchSender.send(TopicPartition("test1", 0), testRecordBatch).flatten,
            batchSender.send(TopicPartition("test2", 0), testRecordBatch).flatten,
          ).flatMapN { (_, _) =>
            val expectedTest1Leader = 1
            val expectedTest2Leader = 2
            nodesCalled.get.assertEquals(Set(expectedTest1Leader, expectedTest2Leader))
          }
        }
    yield ()

  }

  test(
    "No more than maxInFlight messages can exist in flight to a single connection"
  ) {
    def testKafkaClient(inFlightMessages: Ref[IO, Map[Int, Int]]): KafkaClient[IO] =
      new KafkaClient[IO]:
        def leastUsedNode: IO[Int] = 0.pure[IO]
        def sendRequest(nodeId: Int, request: KafkaRequest): IO[IO[request.RespT]] =
          inFlightMessages.getAndUpdate(inFlight =>
            inFlight.updatedWith(nodeId)(f => Some(f.getOrElse(0) + 1))
          ) *>
            IO.sleep(100.seconds) *>
            IO(IO(ProduceResponse(List.empty, 0.seconds).asInstanceOf[request.RespT]))
    for
      inFlight <- Ref.of[IO, Map[Int, Int]](Map.empty)
      kafkaClient = testKafkaClient(inFlight)
      control <- TestControl.execute {
        BatchSender
          .resource(3, 50.seconds, 500.seconds, 500.seconds, testMetadata, kafkaClient)
          .use { batchSender =>
            (0 to 5).toList.traverse(_ => batchSender.send(TopicPartition("test1", 0), testRecordBatch))
          }
      }
      _ <- control.tick
      _ <- inFlight.get.assertEquals(Map(1 -> 3))
      _ <- control.advanceAndTick(100.seconds)
      _ <- inFlight.get.assertEquals(Map(1 -> 2))
    yield ()
  }

  test("An error is returned if blocking for more than maxBlockTime") {
    for {
      control <- TestControl.execute {
        BatchSender.resource(2, 50.seconds, 50.seconds, 50.seconds, testMetadata, noopKafkaClient)
          .use { batchSender => 
            batchSender.send(TopicPartition("test1", 0), testRecordBatch).flatten
          }
      }
      _ <- control.tick
      _ <- control.results.assertEquals(None)
      _ <- control.advanceAndTick(51.seconds)
      _ <- control.results.assertEquals(Outcome.Errored[IO, Throwable, RecordMetadata](new Exception("timeout")))
    } yield ()

  }

object BatchSenderSuite:

  def testMetadata: Metadata[IO] = new Metadata[IO]:
    def allNodes: IO[Map[Int, Node]] =
      Map(
        1 -> Node(1, host"localhost", port"5432"),
        2 -> Node(2, host"localhost", port"5432"),
        3 -> Node(3, host"localhost", port"5432")
      ).pure[IO]

    def currentLeader(topicPartition: TopicPartition): IO[Int] = topicPartition match
      case TopicPartition("test1", 0) => 1.pure[IO]
      case TopicPartition("test2", 0) => 2.pure[IO]
      case _ => 3.pure[IO]

  def noopKafkaClient: KafkaClient[IO] = 
    new KafkaClient[IO] {
      def leastUsedNode: IO[Int] = 0.pure[IO]
      def sendRequest(nodeId: Int, request: KafkaRequest): IO[IO[request.RespT]] = 
        IO.unit.foreverM.as(IO(ProduceResponse(List.empty, 0.seconds).asInstanceOf[request.RespT]))
    }

  val testRecordBatch =
    RecordBatch(
      -1,
      0,
      0,
      0,
      0,
      RecordBatch.Attributes(
        CompressionType.None,
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
      List.empty
    )
