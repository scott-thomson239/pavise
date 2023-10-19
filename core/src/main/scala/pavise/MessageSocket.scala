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

object MessageSocket:
  def apply[F[_]: Async](
      sg: SocketGroup[F],
      host: Host,
      port: Port
  ): Pipe[F, RequestMessage, ResponseMessage] = in =>
    Stream.resource(sg.client(SocketAddress(host, port))).flatMap { socket =>
      socket.reads
        .through(responseStreamDecoder.toPipeByte)
        .concurrently(in.through(requestStreamEncoder.toPipeByte).through(socket.writes))
    }

  val requestStreamEncoder: StreamEncoder[RequestMessage] = 
    StreamEncoder.many(variableSizeBytes(int32, Codec[RequestMessage]))

  val responseStreamDecoder: StreamDecoder[ResponseMessage] =
    StreamDecoder
      .many(int32)
      .flatMap(nBytes =>
        StreamDecoder.isolate(nBytes * 8)(StreamDecoder.many(Codec[ResponseMessage]))
      )
