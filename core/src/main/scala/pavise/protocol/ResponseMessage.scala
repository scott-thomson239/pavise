package pavise.protocol

import scodec.*
import scodec.codecs.*
import scodec.bits.BitVector
import pavise.protocol.KafkaRequest.*
import pavise.protocol.message.ProduceResponse
import pavise.protocol.message.MetadataRequest
import cats.MonadThrow
import cats.syntax.all.*
import pavise.protocol.message.MetadataResponse
import cats.effect.kernel.Async

case class ResponseMessage(correlationId: Int, responseRaw: BitVector)

trait KafkaResponse

object KafkaResponse:

  def decodeFromApiKey[F[_]: MonadThrow](apiKey: Int, raw: BitVector)(using
      ApiVersions
  ): F[KafkaResponse] = apiKey match
    case PRODUCE => ProduceResponse.codec.decodeValue(raw).toOption.liftTo(new Exception("sdfdf"))
    case METADATA => MetadataResponse.codec.decodeValue(raw).toOption.liftTo(new Exception("sfdsf"))
    case _ => MonadThrow[F].raiseError(new Exception("wrong apikey"))

  // def codec(apiKey: Int)(using ApiVersions): Codec[KafkaResponse] = apiKey match {
  //   case PRODUCE => ProduceResponse.codec
  //   case METADATA => MetadataRequest.codec
  // }

object ResponseMessage:

  given codec(using ApiVersions): Codec[ResponseMessage] =
    (int32 :: Codec[BitVector]).as[ResponseMessage]
