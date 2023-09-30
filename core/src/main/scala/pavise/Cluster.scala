package pavise

import com.comcast.ip4s.Host
import com.comcast.ip4s.Port

case class Cluster(
    nodes: Map[Int, Node],
    unauthorizedTopics: Set[String],
    invalidTopics: Set[String],
    internalTopics: Set[String],
    controller: Node,
    partitionByTopicPartition: Map[TopicPartition, PartitionInfo],
    partitionsByTopic: Map[String, List[PartitionInfo]],
    partitionsByNode: Map[Int, List[PartitionInfo]],
)

case class TopicPartition(topic: String, partition: Int)

case class PartitionInfo(topic: String, partition: Int, leader: Node, replicas: List[Node], inSync: List[Node], offline: List[Node])

case class Node(id: Int, host: Host, port: Port)
