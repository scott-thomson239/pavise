package pavise.protocol.message

import pavise.protocol.ErrorCode
import pavise.protocol.KafkaResponse
import scodec.codecs.*
import scala.concurrent.duration.FiniteDuration
import scodec.Codec
import pavise.protocol.HelperCodecs

case class ApiVersionsResponse(
    errorCode: ErrorCode,
    apiKeys: List[ApiVersionsResponse.ApiKey],
    throttleTimeMs: FiniteDuration
) extends KafkaResponse {}

object ApiVersionsResponse:

  given codec: Codec[ApiVersionsResponse] =
    val apikey = (int16 :: int16 :: int16).as[ApiKey]
    (ErrorCode.codec :: list(apikey) :: HelperCodecs.ms).as[ApiVersionsResponse]

  case class ApiKey(
      apiKey: Int,
      minVersion: Int,
      maxVersion: Int
  )
