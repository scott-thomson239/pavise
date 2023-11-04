package pavise.protocol

import scodec.*
import scodec.codecs.*
import scodec.Attempt.*
import scodec.bits.*
import scala.concurrent.duration.*

object HelperCodecs:

  def array[A](inner: Codec[A]): Codec[List[A]] = listOfN(int32, inner)

  val string: Codec[String] = variableSizeBytes(int16, utf8)

  val ms: Codec[FiniteDuration] = int32.xmap(ms => ms.milliseconds, fd => fd.toMillis.toInt)

  val optionalTimestamp: Codec[Option[FiniteDuration]] =
    int64.xmap(
      ms => if ms == -1 then None else Some(ms.milliseconds),
      fd => fd.map(_.toMillis).getOrElse(-1)
    )

  val timestamp: Codec[FiniteDuration] = // maybe wrong
    int64.xmap(ms => ms.milliseconds, fd => fd.toMillis)

  val nullableString: Codec[Option[String]] =
    int16.consume(len => conditional(len > -1, fixedSizeBytes(len, utf8)))(s =>
      s.map(_.size).getOrElse(-1)
    )

  val varint: Codec[Int] = vint

  val varlong: Codec[Long] = vlong
