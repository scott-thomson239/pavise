package pavise

import cats.effect.kernel.Resource
import scala.concurrent.duration.FiniteDuration
import pavise.protocol.KafkaRequest
import com.comcast.ip4s.IpAddress
import cats.effect.kernel.Async
import fs2.io.net.Network
import pavise.protocol.KafkaResponse
import fs2.*
import cats.syntax.all.*
import pavise.protocol.ResponseMessage
import pavise.protocol.RequestMessage

trait KafkaClient[F[_]]:
  def sendRequest(node: Node, request: KafkaRequest): F[F[request.RespT]]

object KafkaClient:
  def resource[F[_]: Async: Network](
      clientId: String,
      bootstrapServers: List[IpAddress],
      requestTimeout: FiniteDuration,
      connectionsMaxIdle: FiniteDuration,
      reconnectBackoff: FiniteDuration,
      reconnectBackoffMax: FiniteDuration,
      socketConnectionSetupTimeout: FiniteDuration,
      socketConnectionSetupTimeoutMax: FiniteDuration
  ): Resource[F, KafkaClient[F]] =
    KeyedResultStream
      .resource[F, Int, Int, RequestMessage, ResponseMessage]()
      .map { keyResStream =>
        new KafkaClient[F]:
          def sendRequest(node: Node, request: KafkaRequest): F[F[request.RespT]] =
            val pipe: Pipe[F, RequestMessage, ResponseMessage] =
              in => in.through(MessageSocket(Network[F], node.host, node.port))

            val reqMessage: RequestMessage = RequestMessage(0, 0, 0, None, request)
            keyResStream
              .sendTo_(node.id, reqMessage, pipe)
              .map(_.flatMap { resp =>
                resp.correlationId match
                  case id if id == reqMessage.correlationId =>
                    resp.response.asInstanceOf[request.RespT].pure[F]
                  case _ =>
                    Async[F].raiseError[request.RespT](new Exception("wrong response type"))
              })
      }
