package pavise

import scodec.bits.ByteVector
import pavise.Serializer.KeyOrValue
import pavise.Serializer.*

trait GenericSerializer[-T <: KeyOrValue, F[_], A]:
  def serialize(topic: String, headers: List[Header], a: A): F[ByteVector]

type KeySerializer[F[_], A] = GenericSerializer[Key, F, A]
type ValueSerializer[F[_], A] = GenericSerializer[Value, F, A]

object Serializer:
  sealed trait KeyOrValue
  sealed trait Key extends KeyOrValue
  sealed trait Value extends KeyOrValue
