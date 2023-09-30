package pavise.producer

import pavise.Cluster

trait Partitioner[F[_], K]:

  def partition(topic: String, key: K, cluster: Cluster): F[Int]

object Partitioner:
  
  def stickyPartitioner[F[_], K]: Partitioner[F, K] = ???
