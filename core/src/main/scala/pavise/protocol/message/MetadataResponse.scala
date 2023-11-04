package pavise.protocol.message

import pavise.protocol.ErrorCode
import scodec.Codec
import pavise.protocol.KafkaResponse
import com.comcast.ip4s.*
import scodec.codecs.*
import pavise.protocol.HelperCodecs
import pavise.protocol.ApiVersions

case class MetadataResponse(
    throttleTimeMs: Int,
    brokers: List[MetadataResponse.Broker],
    clusterId: Option[String],
    controllerId: Int,
    topics: List[MetadataResponse.TopicMetadata],
    clusterAuthorizedOperations: Int
) extends KafkaResponse

object MetadataResponse:

  given codec(using apiVersions: ApiVersions): Codec[MetadataResponse] =
    apiVersions.syncGroup match
      case _ => // 8
        val broker = (int32 :: utf8 :: int32 :: HelperCodecs.nullableString).as[Broker]
        val partitionMetadata = (ErrorCode.codec :: int32 :: int32 :: int32 :: listOfN(int32,
          int32
        ) :: listOfN(int32, int32) :: listOfN(int32, int32)).as[PartitionMetadata]
        val topicMetadata =
          (ErrorCode.codec :: utf8 :: bool :: listOfN(int32, partitionMetadata) :: int32)
            .as[TopicMetadata]
        (int32 :: listOfN(int32, broker) :: HelperCodecs.nullableString :: int32 :: listOfN(int32,
          topicMetadata
        ) :: int32).as[MetadataResponse]

  case class Broker(nodeId: Int, host: String, port: Int, rack: Option[String])

  case class TopicMetadata(
      errorCode: ErrorCode,
      name: String,
      isInternal: Boolean,
      partitions: List[PartitionMetadata],
      topicAuthorizedOperations: Int
  )

  case class PartitionMetadata(
      errorCode: ErrorCode,
      partitionIndex: Int,
      leaderId: Int,
      leaderEpoch: Int,
      replicaNodes: List[Int],
      isrNodes: List[Int],
      offlineReplicas: List[Int]
  )
