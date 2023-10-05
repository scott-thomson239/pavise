package pavise.protocol

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
