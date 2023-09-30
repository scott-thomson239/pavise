package pavise

import cats.effect.kernel.Resource
import scala.concurrent.duration.FiniteDuration
import pavise.protocol.KafkaRequest
import com.comcast.ip4s.IpAddress

trait KafkaClient[F[_]]:
  def sendRequest(nodeId: Int, request: KafkaRequest): F[F[request.RespT]]

object KafkaClient:
  def resource[F[_]](
      clientId: String,
      bootstrapServers: List[IpAddress],
      requestTimeout: FiniteDuration,
      connectionsMaxIdle: FiniteDuration,
      reconnectBackoff: FiniteDuration,
      reconnectBackoffMax: FiniteDuration,
      socketConnectionSetupTimeout: FiniteDuration,
      socketConnectionSetupTimeoutMax: FiniteDuration
  ): Resource[F, KafkaClient[F]] = ???
