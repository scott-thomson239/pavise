package pavise

import cats.effect.kernel.Resource
import cats.effect.std.MapRef
import fs2.concurrent.Channel
import cats.effect.kernel.Async
import cats.effect.std.Supervisor
import fs2.*
import cats.syntax.all.*
import cats.effect.std.Queue
import cats.effect.kernel.Deferred

trait KeyedResultStream[F[_], K, I, V, O]:
  def sendTo(key: K, id: I, value: V, pipe: Pipe[F, V, (I, O)]): F[F[O]]

  def sendTo_(key: K, value: V, pipe: Pipe[F, V, O]): F[F[O]]

object KeyedResultStream:
  def resource[F[_]: Async, K, I, V, O](): Resource[F, KeyedResultStream[F, K, I, V, O]] = (
    Resource.eval(MapRef.inConcurrentHashMap[F, F, K, ResultStream[F, I, V, O]]()),
    Supervisor[F](await = true)
  ).mapN { (chMap, supervisor) =>
    new KeyedResultStream[F, K, I, V, O]:
      def sendTo(key: K, id: I, value: V, pipe: Pipe[F, V, (I, O)]): F[F[O]] = for 
        newCh <- Channel.unbounded[F, V]
        newDefQueue <- Queue.unbounded[F, Deferred[F, O]]
        newResultStream = ResultStream(newCh, Map(id -> newDefQueue))
        resStreamOpt <- chMap(key).getAndUpdate { res => 
          res.getOrElse(newResultStream).some
        }



      yield ()

  }

  case class ResultStream[F[_], I, V, O](ch: Channel[F, V], defMap: Map[I, Queue[F, Deferred[F, O]]])

object StreamMap:
  def resource[F[_]: Async, K, U, V, O](maxPerId: Int): Resource[F, StreamMap[F, K, U, V, O]] =
    for
      channelMap <- Resource.eval(MapRef.inConcurrentHashMap[F, F, K, Channel[F, V]]())
      defMap <- Resource.eval(MapRef.inConcurrentHashMap[F, F, U, Queue[F, Deferred[F, O]]]())
      supervisor <- Supervisor[F](true)
    yield new StreamMap[F, K, U, V, O]:
      def sendTo(key: K, id: U, pipe: Pipe[F, V, (U, O)]): F[F[O]] = for
        newCh <- Channel.unbounded[F, V]
        newDefQueue <- Queue.unbounded[F, Deferred[F, O]]
        chOpt <- channelMap(key).getAndUpdate { ch =>
          ch.getOrElse(newCh).some
        }
        getDefQueue = defMap(id).modify { dOpt =>
          val d = dOpt.getOrElse(newDefQueue)
          (Some(d), d)
        }
        ch <- chOpt.fold(
          supervisor
            .supervise(
              newCh.stream
                .through(pipe)
                .evalMap { case (id, out) =>
                  getDefQueue.flatMap { q =>
                    q.tryTake.flatMap {
                      case Some(d) => d.complete(out)
                      case None => Async[F].raiseError(new Exception("out of order responses"))
                    }
                  }
                }
                .compile
                .drain
            )
            .as(newCh)
        )(_.pure[F])
        newDef <- Deferred[F, O]
      yield ()

  case class OutStream[F[_], V, O](ch: Channel[F, V], defQueue: Queue[F, Deferred[F, O]])
