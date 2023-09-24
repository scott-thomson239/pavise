package pavise.protocol

import scodec.*
import scodec.codecs.*

case class ResponseMessage(correlationId: Int, response: KafkaResponse)

trait KafkaResponse:
  val apiKey: Int

object ResponseMessage:

  given codec: Codec[ResponseMessage] = ???
//    (int32 :: Codec[A]).as[ResponseMessage[A]]
