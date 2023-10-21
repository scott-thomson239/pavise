package pavise.protocol

import pavise.protocol.message.ApiVersionsResponse
import cats.effect.kernel.Async
import pavise.protocol.KafkaRequest.*
import cats.syntax.all.*

case class ApiVersions(
    magic: Int,
    produce: Int,
    fetch: Int,
    listOffsets: Int,
    metadata: Int,
    offsetCommit: Int,
    offsetFetch: Int,
    findCoordinator: Int,
    joinGroup: Int,
    heartbeat: Int,
    leaveGroup: Int,
    syncGroup: Int,
    describeGroups: Int,
    listGroups: Int,
    saslHandshake: Int
)

object ApiVersions:
  def empty: ApiVersions =
    ApiVersions(
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    )

  def fromApiVersionsResponse[F[_]: Async](resp: ApiVersionsResponse): F[ApiVersions] =
    val keyMap = resp.apiKeys.map(apiKey => (apiKey.apiKey, apiKey.maxVersion)).toMap
    (
      Some(0),
      keyMap.get(PRODUCE),
      keyMap.get(FETCH),
      keyMap.get(LIST_OFFSETS),
      keyMap.get(METADATA),
      keyMap.get(OFFSET_COMMIT),
      keyMap.get(OFFSET_FETCH),
      keyMap.get(FIND_COORDINATOR),
      keyMap.get(JOIN_GROUP),
      keyMap.get(HEARTBEAT),
      keyMap.get(LEAVE_GROUP),
      keyMap.get(SYNC_GROUP),
      keyMap.get(DESCRIBE_GROUPS),
      keyMap.get(LIST_GROUPS),
      keyMap.get(SASL_HANDSHAKE)
    ).mapN(ApiVersions.apply)
      .liftTo(new Exception("missing apikey"))
