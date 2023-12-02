package pavise

import scodec.bits.ByteVector

sealed trait GenericSerializer[F[_], A]:
  def serialize(topic: String, headers: List[Header], a: A): F[ByteVector]

trait KeySerializer[F[_], A] extends GenericSerializer[F, A]
trait ValueSerializer[F[_], A] extends GenericSerializer[F, A]

