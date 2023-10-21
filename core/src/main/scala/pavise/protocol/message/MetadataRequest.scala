package pavise.protocol.message

import scodec.Codec
import pavise.protocol.KafkaRequest
import scodec.codecs.*
import pavise.protocol.ApiVersions
import cats.MonadThrow
import cats.syntax.all.*
import scodec.bits.BitVector

case class MetadataRequest(
    topics: List[String],
    allowAutoTopicCreation: Boolean,
    includeClusterAuthorizedOperations: Boolean,
    includeTopicAuthorizedOperations: Boolean
) extends KafkaRequest:
  type RespT = MetadataResponse
  def parseCorrespondingResponse[F[_]: MonadThrow](resp: BitVector)(using ApiVersions): F[RespT] =
    MetadataResponse.codec.decodeValue(resp).toOption.liftTo(new Exception("failed parse"))

object MetadataRequest:

  given codec(using apiVersions: ApiVersions): Codec[MetadataRequest] =
    apiVersions.syncGroup match
      case _ => (list(utf8) :: bool :: bool :: bool).as[MetadataRequest] // 8
