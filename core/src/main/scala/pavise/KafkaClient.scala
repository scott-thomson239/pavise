package pavise

import cats.effect.kernel.Resource
import scala.concurrent.duration.FiniteDuration
import pavise.protocol.KafkaRequest
import cats.effect.kernel.Async
import cats.effect.syntax.all.*
import fs2.io.net.Network
import fs2.*
import cats.syntax.all.*
import pavise.protocol.ResponseMessage
import pavise.protocol.RequestMessage
import cats.effect.kernel.Ref

trait KafkaClient[F[_]]:
  def sendRequest(nodeId: Int, request: KafkaRequest): F[F[request.RespT]]
  def leastUsedNode: F[Int]

object KafkaClient:

  def resource[F[_]: Async: Network](
      metadata: Metadata[F],
      clientId: String,
      requestTimeout: FiniteDuration,
      connectionsMaxIdle: FiniteDuration,
      reconnectBackoff: FiniteDuration,
      reconnectBackoffMax: FiniteDuration,
      socketConnectionSetupTimeout: FiniteDuration,
      socketConnectionSetupTimeoutMax: FiniteDuration
  ): Resource[F, KafkaClient[F]] =
    (
      KeyedResultStream
        .resource[F, Int, Int, RequestMessage, ResponseMessage](),
      Resource.eval(Ref.of(Map.empty[Int, NodeConnectionState]))
    )
      .mapN { (keyResStream, clusterConnectionState) =>
        new KafkaClient[F]:
          def sendRequest(nodeId: Int, request: KafkaRequest): F[F[request.RespT]] =
            metadata.allNodes
              .flatMap(_.get(nodeId).liftTo[F](new Exception("node doesn't exist")))
              .flatMap { node =>
                val pipe: Pipe[F, RequestMessage, ResponseMessage] =
                  in =>
                    in.through(
                      MessageSocket(Network[F], node.host, node.port)
                    )
                val reqMessage: RequestMessage = RequestMessage(0, 0, 0, None, request)
                keyResStream
                  .sendTo_(nodeId, reqMessage, pipe)
                  .map(_.flatMap { resp =>
                    resp.asInstanceOf[request.RespT].pure[F]
                  })
              }

          def leastUsedNode: F[Int] =
            (metadata.allNodes, clusterConnectionState.get).flatMapN { (allNodes, connectedNodes) =>
              val newNodes = allNodes.keySet.diff(connectedNodes.keySet)
              newNodes.headOption.orElse {
                connectedNodes.keySet.headOption
              }.liftTo(new Exception("no nodes"))
            }
      }

  case class NodeConnectionState(state: ConnectionState)

  enum ConnectionState:
    case Disconnected, Connecting, CheckingApiVersions, Ready, AuthenticationFailure
