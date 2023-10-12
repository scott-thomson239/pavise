package pavise.protocol.message

import scodec.Codec
import pavise.protocol.KafkaRequest
import scodec.codecs.*
import pavise.protocol.ApiVersions

case class MetadataRequest(
    topics: List[String],
    allowAutoTopicCreation: Boolean,
    includeClusterAuthorizedOperations: Boolean,
    includeTopicAuthorizedOperations: Boolean
) extends KafkaRequest:
  type RespT = MetadataResponse

object MetadataRequest:

  given codec(using apiVersions: ApiVersions): Codec[MetadataRequest] =
    apiVersions.syncGroup match
      case _ => (list(utf8) :: bool :: bool :: bool).as[MetadataRequest] // 8
