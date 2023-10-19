package pavise.protocol

import scodec.codecs.*
import scodec.*
import pavise.protocol.message.ProduceRequest
import pavise.protocol.message.MetadataRequest

case class RequestMessage(
    apiKey: Int,
    apiVersion: Int,
    correlationId: Int,
    clientId: Option[String],
    request: KafkaRequest
)

trait KafkaRequest:
  type RespT

object KafkaRequest:

  val PRODUCE = 0
  val FETCH = 1
  val LIST_OFFSETS = 2
  val METADATA = 3
  val OFFSET_COMMIT = 8
  val OFFSET_FETCH = 9
  val FIND_COORDINATOR = 10
  val JOIN_GROUP = 11
  val HEARTBEAT = 12
  val LEAVE_GROUP = 13
  val SYNC_GROUP = 14
  val DESCRIBE_GROUPS = 15
  val LIST_GROUPS = 16
  val SASL_HANDSHAKE = 17
  val API_VERSIONS = 18

  given codec(using ApiVersions): Codec[KafkaRequest] = 
    discriminated[KafkaRequest].by(int16)
      .typecase(PRODUCE, ProduceRequest.codec)
      .typecase(METADATA, MetadataRequest.codec)

object RequestMessage:

  given codec(using ApiVersions): Codec[RequestMessage] =
   (int16 :: int16 :: int32 :: HelperCodecs.nullableString :: Codec[KafkaRequest]).as[RequestMessage]
