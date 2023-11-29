package pavise.protocol

import scala.concurrent.duration.FiniteDuration
import scodec.bits.ByteVector
import scodec.Codec
import scodec.Attempt
import scodec.*
import scodec.codecs.*
import scodec.Attempt.*
import pavise.Header
import pavise.CompressionType
import pavise.CompressionType.*

case class RecordBatch(
    baseOffset: Long,
    batchLength: Int,
    partitionLeaderEpoch: Int,
    magic: Int,
    crc: Int,
    attributes: RecordBatch.Attributes,
    lastOffsetDelta: Int,
    baseTimestamp: FiniteDuration,
    maxTimestamp: FiniteDuration,
    producerId: Long,
    producerEpoch: Int,
    baseSequence: Int,
    records: List[Record]
)

object RecordBatch {

  given codec: Codec[RecordBatch] =
    (int64 :: int32 :: int32 :: int8 :: int32 :: attributesCodec :: int32 :: HelperCodecs.timestamp :: HelperCodecs.timestamp :: int64 :: int16 :: int32 :: variableSizeBytes(int32, list(
      Record.codec
    ))).as[RecordBatch]

  case class Attributes(
      compression: CompressionType,
      timestampType: Boolean,
      isTransactional: Boolean,
      isControlBatch: Boolean,
      hasDeleteHorizonMs: Boolean
  )

  val attributesCodec =
    (compCodec :: bool :: bool :: bool :: bool).as[Attributes]

  val compCodec: Codec[CompressionType] = uint2.exmap(fromRaw, toRaw)

  def toRaw(comp: CompressionType): Attempt[Int] = comp match {
    case None => successful(0)
    case GZip => successful(1)
    case Snappy => successful(2)
    case Lz4 => successful(3)
    case Zstd => successful(4)
  }

  def fromRaw(comp: Int): Attempt[CompressionType] = comp match {
    case 0 => successful(None)
    case 1 => successful(GZip)
    case 2 => successful(Snappy)
    case 3 => successful(Lz4)
    case 4 => successful(Zstd)
  }
}

case class Record(
    timestampDelta: FiniteDuration,
    offsetDelta: Int,
    key: ByteVector,
    value: ByteVector,
    headers: List[Header]
)

object Record {

  def apply(
      timestampDelta: FiniteDuration,
      offsetDelta: Int,
      key: ByteVector,
      value: ByteVector,
      headers: List[Header]
  ): Record =
    Record(
      timestampDelta,
      offsetDelta,
      key,
      value,
      headers
    )

  val codec: Codec[Record] =
    variableSizeBytes(HelperCodecs.varint, (HelperCodecs.ms :: HelperCodecs.varint :: variableSizeBytes(
      HelperCodecs.varint,
      bytes
    ) :: variableSizeBytes(HelperCodecs.varint, bytes) :: variableSizeBytes(
      int32,
      list(
        headerCodec
      )
    )).as[Record])

  val headerCodec: Codec[Header] =
    (variableSizeBytes(HelperCodecs.varint, utf8) :: variableSizeBytes(
      HelperCodecs.varint,
      bytes
    )).as[Header]

}
