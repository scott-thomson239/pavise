package pavise.protocol

import scodec.*
import scodec.codecs.*

case class ResponseMessage(correlationId: Int, response: KafkaResponse)

trait KafkaResponse

object KafkaResponse:

  given codec(using ApiVersions): Codec[KafkaResponse] = ???

object ResponseMessage:

  given codec(using ApiVersions): Codec[ResponseMessage] = 
    (int32 :: Codec[KafkaResponse]).as[ResponseMessage]
