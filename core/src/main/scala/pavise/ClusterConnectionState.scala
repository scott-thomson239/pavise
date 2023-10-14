package pavise

case class ClusterConnectionState()

object ClusterConnectionState:
  
  case class NodeConnectionState()

  enum ConnectionState:
    case Disconnected, Connecting, CheckingApiVersions, Ready, AuthenticationFailure
