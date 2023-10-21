package pavise.protocol.message

import pavise.protocol.KafkaRequest
import scodec.codecs.*
import pavise.protocol.ApiVersions
import scodec.Codec

case class ApiVersionsRequest() extends KafkaRequest:
  type RespT = ApiVersionsResponse

object ApiVersionsRequest:

  given codec: Codec[ApiVersionsRequest] =
    provide(ApiVersionsRequest())
