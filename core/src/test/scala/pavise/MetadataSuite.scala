package pavise

import munit.CatsEffectSuite
import cats.effect.IO
import com.comcast.ip4s.SocketAddress
import com.comcast.ip4s.*
import scala.concurrent.duration.*
import cats.effect.kernel.Resource
import fs2.concurrent.SignallingRef
import cats.effect.testkit.TestControl
import cats.effect.kernel.Outcome

class MetadataSuite extends CatsEffectSuite:

  def metadataResource(
      metadataMaxAge: FiniteDuration,
      metadataMaxIdle: FiniteDuration,
      bootstrapServers: List[SocketAddress[Host]] = List(
        SocketAddress(ipv4"127.0.0.1", port"5555")
      )
  ): Resource[IO, (SignallingRef[IO, ClusterState], Metadata[IO])] =
    for
      cs <- Resource.eval(ClusterState[IO])
      metadata <- Metadata.resource[IO](bootstrapServers, metadataMaxAge, metadataMaxIdle, cs)
      _ <- Resource.eval(cs.update { state =>
        val node1 = Node(0, host"localhost", port"5555")
        val node2 = Node(1, host"localhost", port"5555")
        val partitionInfo1 = PartitionInfo("test1", 0, node1, List(), List(), List())
        val partitionInfo2 = PartitionInfo("test1", 1, node2, List(), List(), List())
        state.copy(cluster =
          state.cluster.copy(
            nodes = Map(
              0 -> node1,
              1 -> node2
            ),
            partitionByTopicPartition = Map(
              TopicPartition("test1", 0) -> partitionInfo1,
              TopicPartition("test1", 1) -> partitionInfo2
            ),
            partitionsByNode = Map(
              0 -> List(partitionInfo1),
              1 -> List(partitionInfo2)
            )
          )
        )
      })
    yield (cs, metadata)

  test("currentLeader will fetch current leader node of existing topic partition") {
    metadataResource(10.seconds, 99.seconds).use { (clusterSig, metadata) =>
      val expectedNodeId1 = 0
      val expectedNodeId2 = 1
      metadata.currentLeader(TopicPartition("test1", 0)).assertEquals(expectedNodeId1) *>
      metadata.currentLeader(TopicPartition("test1", 1)).assertEquals(expectedNodeId2)
    }
  }

  test(
    "currentLeader will timeout after waiting for leader node of absent topic for maxBlockTime"
  ) {
    TestControl.execute {
      metadataResource(10.seconds, 99.seconds).use { (clusterSig, metadata) =>
        metadata.currentLeader(TopicPartition("missingTopic", 99))
      }
    }.flatMap { control =>
      for {
        _ <- control.results.assertEquals(None)
        _ <- control.tickAll
        _ <- control.results.assertEquals(Outcome.Errored(new Exception("timeout")))
      } yield ()
      
    }
  }

  test("allNodes fetches a map of the current nodes that exist within metadata") {
    metadataResource(10.seconds, 99.seconds).use { (clusterSig, metadata) => 
      metadata.allNodes.assertEquals(Map(
        0 -> Node(0, host"localhost", port"5555"),
        1 -> Node(1, host"localhost", port"5555")
      ))
    }
  }

  test("metadata will be refreshed after metadataMaxAge seconds") {
    TestControl.execute {
      metadataResource(10.seconds, 99.seconds).use { (clusterSig, _) => 
        clusterSig.waitUntil(c => c.waitingTopics.contains("test1"))
      }
    }.flatMap { control => 
      for {
        _ <- control.results.assertEquals(None)
        _ <- control.tick
        _ <- control.results.assertEquals(Outcome.Succeeded(IO(true)))
      } yield ()
    }
  }

  test(
    "metadata for a topic that has not called currentLeader will be deleted after metadataMaxIdle seconds"
  ) {
    TestControl.executeEmbed {
      metadataResource(10.seconds, 99.seconds).use { (clusterSig, _) => 
        IO.sleep(99.seconds) *> clusterSig.get.map(_.cluster.partitionsByTopic.get("test1"))
      }
    }.assertEquals(None)
  }
