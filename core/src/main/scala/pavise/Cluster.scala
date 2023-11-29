package pavise

import com.comcast.ip4s.Host
import com.comcast.ip4s.Port
import fs2.concurrent.Signal
import fs2.concurrent.SignallingRef

case class Cluster(
    nodes: Map[Int, Node],
    unauthorizedTopics: Set[String],
    invalidTopics: Set[String],
    internalTopics: Set[String],
    controller: Option[Node],
    partitionByTopicPartition: Map[TopicPartition, PartitionInfo],
    partitionsByTopic: Map[String, List[PartitionInfo]],
    partitionsByNode: Map[Int, List[PartitionInfo]]
)

object Cluster:
  def empty: Cluster =
    Cluster(Map.empty, Set.empty, Set.empty, Set.empty, None, Map.empty, Map.empty, Map.empty)

case class TopicPartition(topic: String, partition: Int)

case class PartitionInfo(
    topic: String,
    partition: Int,
    leader: Node,
    replicas: List[Node],
    inSync: List[Node],
    offline: List[Node]
)

case class Node(id: Int, host: Host, port: Port)
