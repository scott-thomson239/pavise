package pavise.protocol

import scodec.*
import scodec.codecs.*
import scodec.Attempt.*

enum ErrorCode {
  case Unknown, None, OffsetOutOfRange, CorruptMessage, UnknownTopicOrPartition,
    InvalidFetchSize, LeaderNotAvailable, NotLeaderForPartition, RequestTimedOut,
    BrokerNotAvailable, ReplicaNotAvailable, MessageTooLarge, StaleControllerEpoch,
    OffsetMetadataTooLarge, NetworkException, GroupLoadInProgress, GroupCoordinatorNotAvailable,
    NotCoordinatorForGroup, InvalidTopicException, RecordListTooLarge, NotEnoughReplicas,
    NotEnoughReplicasAfterAppend, InvalidRequiredAcks, IllegalGeneration,
    InconsistentGroupProtocol, InvalidGroupId, UnknownMemberId, InvalidSessionTimeout,
    RebalanceInProgress, InvalidCommitOffsetSize, TopicAuthorizationFailed,
    GroupAuthorizationFailed, ClusterAuthorizationFailed, InvalidTimestamp,
    UnsupportedSaslMechanism, IllegalSaslState, UnsupportedVersion
}

object ErrorCode {
  val UNKNOWN = -1
  val NONE = 0
  val OFFSET_OUT_OF_RANGE = 1
  val CORRUPT_MESSAGE = 2
  val UNKNOWN_TOPIC_OR_PARTITION = 3
  val INVALID_FETCH_SIZE = 4
  val LEADER_NOT_AVAILABLE = 5
  val NOT_LEADER_FOR_PARTITION = 6
  val REQUEST_TIMED_OUT = 7
  val BROKER_NOT_AVAILABLE = 8
  val REPLICA_NOT_AVAILABLE = 9
  val MESSAGE_TOO_LARGE = 10
  val STALE_CONTROLLER_EPOCH = 11
  val OFFSET_METADATA_TOO_LARGE = 12
  val NETWORK_EXCEPTION = 13
  val GROUP_LOAD_IN_PROGRESS = 14
  val GROUP_COORDINATOR_NOT_AVAILABLE = 15
  val NOT_COORDINATOR_FOR_GROUP = 16
  val INVALID_TOPIC_EXCEPTION = 17
  val RECORD_LIST_TOO_LARGE = 18
  val NOT_ENOUGH_REPLICAS = 19
  val NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20
  val INVALID_REQUIRED_ACKS = 21
  val ILLEGAL_GENERATION = 22
  val INCONSISTENT_GROUP_PROTOCOL = 23
  val INVALID_GROUP_ID = 24
  val UNKNOWN_MEMBER_ID = 25
  val INVALID_SESSION_TIMEOUT = 26
  val REBALANCE_IN_PROGRESS = 27
  val INVALID_COMMIT_OFFSET_SIZE = 28
  val TOPIC_AUTHORIZATION_FAILED = 29
  val GROUP_AUTHORIZATION_FAILED = 30
  val CLUSTER_AUTHORIZATION_FAILED = 31
  val INVALID_TIMESTAMP = 32
  val UNSUPPORTED_SASL_MECHANISM = 33
  val ILLEGAL_SASL_STATE = 34
  val UNSUPPORTED_VERSION = 35

  val codec: Codec[ErrorCode] = uint16.exmap(fromRaw, toRaw)

  private def fromRaw(errorCode: Int): Attempt[ErrorCode] = errorCode match {
    case UNKNOWN => successful(Unknown)
    case NONE => successful(None)
    case OFFSET_OUT_OF_RANGE => successful(OffsetOutOfRange)
    case CORRUPT_MESSAGE => successful(CorruptMessage)
    case UNKNOWN_TOPIC_OR_PARTITION => successful(UnknownTopicOrPartition)
    case INVALID_FETCH_SIZE => successful(InvalidFetchSize)
    case LEADER_NOT_AVAILABLE => successful(LeaderNotAvailable)
    case NOT_LEADER_FOR_PARTITION => successful(NotLeaderForPartition)
    case REQUEST_TIMED_OUT => successful(RequestTimedOut)
    case BROKER_NOT_AVAILABLE => successful(BrokerNotAvailable)
    case REPLICA_NOT_AVAILABLE => successful(ReplicaNotAvailable)
    case MESSAGE_TOO_LARGE => successful(MessageTooLarge)
    case STALE_CONTROLLER_EPOCH => successful(StaleControllerEpoch)
    case OFFSET_METADATA_TOO_LARGE => successful(OffsetMetadataTooLarge)
    case NETWORK_EXCEPTION => successful(NetworkException)
    case GROUP_LOAD_IN_PROGRESS => successful(GroupLoadInProgress)
    case GROUP_COORDINATOR_NOT_AVAILABLE =>
      successful(GroupCoordinatorNotAvailable)
    case NOT_COORDINATOR_FOR_GROUP => successful(NotCoordinatorForGroup)
    case INVALID_TOPIC_EXCEPTION => successful(InvalidTopicException)
    case RECORD_LIST_TOO_LARGE => successful(RecordListTooLarge)
    case NOT_ENOUGH_REPLICAS => successful(NotEnoughReplicas)
    case NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
      successful(NotEnoughReplicasAfterAppend)
    case INVALID_REQUIRED_ACKS => successful(InvalidRequiredAcks)
    case ILLEGAL_GENERATION => successful(IllegalGeneration)
    case INCONSISTENT_GROUP_PROTOCOL => successful(InconsistentGroupProtocol)
    case INVALID_GROUP_ID => successful(InvalidGroupId)
    case UNKNOWN_MEMBER_ID => successful(UnknownMemberId)
    case INVALID_SESSION_TIMEOUT => successful(InvalidSessionTimeout)
    case REBALANCE_IN_PROGRESS => successful(RebalanceInProgress)
    case INVALID_COMMIT_OFFSET_SIZE => successful(InvalidCommitOffsetSize)
    case TOPIC_AUTHORIZATION_FAILED => successful(TopicAuthorizationFailed)
    case GROUP_AUTHORIZATION_FAILED => successful(GroupAuthorizationFailed)
    case CLUSTER_AUTHORIZATION_FAILED => successful(ClusterAuthorizationFailed)
    case INVALID_TIMESTAMP => successful(InvalidTimestamp)
    case UNSUPPORTED_SASL_MECHANISM => successful(UnsupportedSaslMechanism)
    case ILLEGAL_SASL_STATE => successful(IllegalSaslState)
    case UNSUPPORTED_VERSION => successful(UnsupportedVersion)
    case _ => failure(Err("Unknown error code"))
  }

  private def toRaw(errorCode: ErrorCode): Attempt[Int] = errorCode match {
    case Unknown => successful(UNKNOWN)
    case None => successful(NONE)
    case OffsetOutOfRange => successful(OFFSET_OUT_OF_RANGE)
    case CorruptMessage => successful(CORRUPT_MESSAGE)
    case UnknownTopicOrPartition => successful(UNKNOWN_TOPIC_OR_PARTITION)
    case InvalidFetchSize => successful(INVALID_FETCH_SIZE)
    case LeaderNotAvailable => successful(LEADER_NOT_AVAILABLE)
    case NotLeaderForPartition => successful(NOT_LEADER_FOR_PARTITION)
    case RequestTimedOut => successful(REQUEST_TIMED_OUT)
    case BrokerNotAvailable => successful(BROKER_NOT_AVAILABLE)
    case ReplicaNotAvailable => successful(REPLICA_NOT_AVAILABLE)
    case MessageTooLarge => successful(MESSAGE_TOO_LARGE)
    case StaleControllerEpoch => successful(STALE_CONTROLLER_EPOCH)
    case OffsetMetadataTooLarge => successful(OFFSET_METADATA_TOO_LARGE)
    case NetworkException => successful(NETWORK_EXCEPTION)
    case GroupLoadInProgress => successful(GROUP_LOAD_IN_PROGRESS)
    case GroupCoordinatorNotAvailable =>
      successful(GROUP_COORDINATOR_NOT_AVAILABLE)
    case NotCoordinatorForGroup => successful(NOT_COORDINATOR_FOR_GROUP)
    case InvalidTopicException => successful(INVALID_TOPIC_EXCEPTION)
    case RecordListTooLarge => successful(RECORD_LIST_TOO_LARGE)
    case NotEnoughReplicas => successful(NOT_ENOUGH_REPLICAS)
    case NotEnoughReplicasAfterAppend =>
      successful(NOT_ENOUGH_REPLICAS_AFTER_APPEND)
    case InvalidRequiredAcks => successful(INVALID_REQUIRED_ACKS)
    case IllegalGeneration => successful(ILLEGAL_GENERATION)
    case InconsistentGroupProtocol => successful(INCONSISTENT_GROUP_PROTOCOL)
    case InvalidGroupId => successful(INVALID_GROUP_ID)
    case UnknownMemberId => successful(UNKNOWN_MEMBER_ID)
    case InvalidSessionTimeout => successful(INVALID_SESSION_TIMEOUT)
    case RebalanceInProgress => successful(REBALANCE_IN_PROGRESS)
    case InvalidCommitOffsetSize => successful(INVALID_COMMIT_OFFSET_SIZE)
    case TopicAuthorizationFailed => successful(TOPIC_AUTHORIZATION_FAILED)
    case GroupAuthorizationFailed => successful(GROUP_AUTHORIZATION_FAILED)
    case ClusterAuthorizationFailed => successful(CLUSTER_AUTHORIZATION_FAILED)
    case InvalidTimestamp => successful(INVALID_TIMESTAMP)
    case UnsupportedSaslMechanism => successful(UNSUPPORTED_SASL_MECHANISM)
    case IllegalSaslState => successful(ILLEGAL_SASL_STATE)
    case UnsupportedVersion => successful(UNSUPPORTED_VERSION)
  }

}
