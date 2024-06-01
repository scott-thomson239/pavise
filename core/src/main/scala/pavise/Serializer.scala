package pavise

import scodec.bits.ByteVector
import cats.effect.kernel.Sync

sealed trait Serializer[F[_], A]:
  def serialize(topic: String, headers: List[Header], a: A): F[ByteVector]

trait KeySerializer[F[_], A] extends Serializer[F, A]
trait ValueSerializer[F[_], A] extends Serializer[F, A]

object Serializer:

  def bytes[F[_]](implicit F: Sync[F]): Serializer[F, ByteVector] =
    new Serializer[F, ByteVector]:
      def serialize(topic: String, headers: List[Header], a: ByteVector): F[ByteVector] =
        F.pure(a)

  def string[F[_]](implicit F: Sync[F]): Serializer[F, String] =
    new Serializer[F, String]:
      def serialize(topic: String, headers: List[Header], a: String): F[ByteVector] =
        F.pure(ByteVector(a.getBytes))
