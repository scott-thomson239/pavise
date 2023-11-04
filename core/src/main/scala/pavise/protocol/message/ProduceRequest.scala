package pavise.protocol.message

import scodec.Codec
import pavise.producer.*
import scala.concurrent.duration.FiniteDuration
import pavise.protocol.KafkaRequest
import scodec.codecs.*
import pavise.protocol.ApiVersions
import pavise.protocol.HelperCodecs
import scodec.Attempt
import scodec.*
import scodec.codecs.*
import scodec.Attempt.*
import scodec.bits.*
import cats.MonadThrow
import cats.syntax.all.*

case class ProduceRequest(
    transactionalId: Option[String],
    acks: Acks,
    timeoutMs: FiniteDuration,
    topicData: List[ProduceRequest.TopicData]
) extends KafkaRequest:
  type RespT = ProduceResponse

object ProduceRequest:

  given codec(using apiVersions: ApiVersions): Codec[ProduceRequest] =
    apiVersions.syncGroup match
      case _ => // 8
        val partitionData = (int32 :: bytes).as[PartitionData]
        val topicData = (utf8 :: listOfN(int32, partitionData)).as[TopicData]
        (HelperCodecs.nullableString :: acksCodec :: HelperCodecs.ms :: list(topicData))
          .as[ProduceRequest]

  case class TopicData(name: String, partitionData: List[PartitionData])

  case class PartitionData(index: Int, records: ByteVector)

  val acksCodec: Codec[Acks] = uint16.exmap(fromRaw, toRaw)

  def toRaw(acks: Acks): Attempt[Int] = acks match
    case Acks.Zero => successful(0)
    case Acks.One => successful(1)
    case Acks.All => successful(-1)

  def fromRaw(acks: Int): Attempt[Acks] = acks match
    case 0 => successful(Acks.Zero)
    case 1 => successful(Acks.One)
    case -1 => successful(Acks.All)
