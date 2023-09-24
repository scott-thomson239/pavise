package pavise.producer

trait Partitioner[F[_], K]:

  def partition(topic: String, key: K): F[Int]

object Partitioner:
  
  def stickyPartitioner[F[_], K]: Partitioner[F, K] = ???
