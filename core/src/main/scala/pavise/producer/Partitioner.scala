package pavise.producer

import pavise.Cluster
import pavise.Metadata

trait Partitioner[F[_], K]:

  def partition(topic: String, key: K, metadata: Metadata[F]): F[Int]

object Partitioner:
  
  def stickyPartitioner[F[_], K]: Partitioner[F, K] = ???
