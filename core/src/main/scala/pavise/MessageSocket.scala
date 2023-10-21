package pavise

import cats.effect.kernel.Async
import pavise.protocol.ResponseMessage
import pavise.protocol.RequestMessage
import com.comcast.ip4s.Port
import com.comcast.ip4s.Host
import fs2.io.net.SocketGroup
import fs2.*
import fs2.interop.scodec.StreamEncoder
import scodec.*
import fs2.interop.scodec.StreamDecoder
import com.comcast.ip4s.SocketAddress
import scodec.codecs.*
import pavise.protocol.ApiVersions
import pavise.protocol.KafkaResponse
import pavise.protocol.KafkaRequest
import pavise.protocol.message.*
import fs2.io.net.Socket

object MessageSocket:

  def apply[F[_]: Async](
      sg: SocketGroup[F],
      host: Host,
      port: Port
  ): Pipe[F, RequestMessage, KafkaResponse] =
    in => // make sure can use "in" in two places correctly
      Stream.resource(sg.client(SocketAddress(host, port))).flatMap { socket =>
        val apiVersionsRequest = ApiVersionsRequest()
        brokerStream(socket, Stream.emit[F, KafkaRequest](apiVersionsRequest))(using ApiVersions.empty)
          .evalMap(resp => ApiVersions.fromResponse(resp.asInstanceOf[ApiVersionsResponse]))
          .head
          .flatMap { apiVersions =>
            brokerStream(socket, in)(using apiVersions)
          }
      }

  private def brokerStream[F[_]: Async](
      socket: Socket[F],
      in: Stream[F, KafkaRequest]
  )(using ApiVersions): Stream[F, KafkaResponse] =
    val requestIn = in.map(kreq => RequestMessage(0, 0, 0, None, kreq))
    socket.reads
      .through(responseStreamDecoder.toPipeByte)
      .zip(requestIn)
      .flatMap { case (resp, req) =>
        if req.correlationId == resp.correlationId then
          Stream.eval(KafkaResponse.decodeFromApiKey(req.apiKey, resp.responseRaw))
        else Stream.raiseError(new Exception("cor id doesn't match"))
      }
      .concurrently(
          requestIn
          .through(requestStreamEncoder.toPipeByte)
          .through(socket.writes)
      )

  private def requestStreamEncoder(using ApiVersions): StreamEncoder[RequestMessage] =
    StreamEncoder.many(variableSizeBytes(int32, Codec[RequestMessage]))

  private def responseStreamDecoder(using ApiVersions): StreamDecoder[ResponseMessage] =
    StreamDecoder
      .many(int32)
      .flatMap(nBytes =>
        StreamDecoder.isolate(nBytes * 8)(StreamDecoder.once(Codec[ResponseMessage]))
      )
