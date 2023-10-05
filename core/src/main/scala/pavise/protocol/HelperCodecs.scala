package pavise.protocol

import scodec.*
import scodec.codecs.*
import scodec.Attempt.*
import scodec.bits.*
import scala.concurrent.duration.FiniteDuration

object HelperCodecs {

  // val MSB = hex"0x80"
  // val LOW_7_BITS = hex"0x7f"
  // val INT_REST_BYTES = hex"0xffffff80"

  // val codec: Codec[Int] = bytes.exmap(decode, encode)

  // private def decode(x: ByteVector): Attempt[Int] = ???

  // private def encode(x: Int): Attempt[ByteVector] = ???
  //
  val ms: Codec[FiniteDuration] = ???

  val timestamp: Codec[FiniteDuration] = ???

  val nullableString: Codec[Option[String]] = ???

  val varint: Codec[Int] = ???

  val varlong: Codec[Long] = ???
}
