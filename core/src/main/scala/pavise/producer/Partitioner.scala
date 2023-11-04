package pavise.producer

import pavise.Cluster
import pavise.Metadata
import cats.syntax.all.*
import cats.effect.kernel.Async

trait Partitioner[F[_], K]:

  def partition(topic: String, key: K, metadata: Metadata[F]): F[Int]

object Partitioner:

  def dummyPartitioner[F[_]: Async, K]: Partitioner[F, K] = 
    new Partitioner[F, K]:
      def partition(topic: String, key: K, metadata: Metadata[F]): F[Int] = 0.pure[F]

  def stickyPartitioner[F[_], K]: Partitioner[F, K] = ???
