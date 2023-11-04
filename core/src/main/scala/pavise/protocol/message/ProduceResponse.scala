package pavise.protocol.message

import pavise.protocol.ErrorCode
import scodec.Codec
import pavise.protocol.KafkaResponse
import scodec.codecs.*
import scala.concurrent.duration.FiniteDuration
import pavise.protocol.HelperCodecs
import pavise.protocol.ApiVersions

case class ProduceResponse(
    responses: List[ProduceResponse.Response],
    throttleTimeMs: FiniteDuration
) extends KafkaResponse

object ProduceResponse:

  given codec(using apiVersions: ApiVersions): Codec[ProduceResponse] =
    apiVersions.syncGroup match
      case _ => // 8
        val recordError = (int32 :: HelperCodecs.nullableString).as[RecordError]
        val partitionResponse =
          (int32 :: ErrorCode.codec :: int64 :: HelperCodecs.optionalTimestamp :: int64 :: list(
            recordError
          ) :: HelperCodecs.nullableString).as[PartitionResponse]
        val response = (utf8 :: listOfN(int32, partitionResponse)).as[Response]
        (listOfN(int32, response) :: HelperCodecs.ms).as[ProduceResponse]

  case class Response(
      name: String,
      partitionResponses: List[PartitionResponse]
  )

  case class PartitionResponse(
      index: Int,
      errorCode: ErrorCode,
      baseOffset: Long,
      logAppendTimeMs: Option[FiniteDuration],
      logStartOffset: Long,
      recordErrors: List[RecordError],
      errorMessage: Option[String]
  )

  case class RecordError(
      batchIndex: Int,
      batchIndexErrorMessage: Option[String]
  )
