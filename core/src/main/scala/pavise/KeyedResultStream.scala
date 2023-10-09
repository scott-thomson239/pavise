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
    Supervisor[F](await = true) // maybe not needed with stream.spawn?
  ).mapN { (chMap, supervisor) =>
    new KeyedResultStream[F, K, I, V, O]:
      def sendTo(key: K, id: I, value: V, pipe: Pipe[F, V, (I, O)]): F[F[O]] = for
        newCh <- Channel.unbounded[F, V]
        newDefQueue <- Queue.unbounded[F, Deferred[F, O]]
        newResStream = ResultStream(newCh, Map(id -> newDefQueue))
        resStreamOpt <- chMap(key).modify { res =>
          res
            .map { a =>
              a.defMap
                .get(id)
                .map(q => (a.some, (a.ch, q).some))
                .getOrElse(
                  (
                    a.copy(defMap = a.defMap.updated(id, newDefQueue)).some,
                    (a.ch, newDefQueue).some
                  )
                )
            }
            .getOrElse((newResStream.some, None))
        }
        X <- resStreamOpt.fold(
          supervisor
            .supervise(
              newCh.stream
                .through(pipe)
                .evalMap { case (id, res) =>
                  newResStream.defMap
                    .get(id)
                    .flatTraverse(_.tryTake)
                    .flatMap { defOpt =>
                      defOpt
                        .liftTo[F](new Exception("out of order"))
                        .flatMap(_.complete(res))
                    }
                }
                .compile
                .drain
            )
            .as((newCh, newDefQueue))
          )(a => a.pure[F])
        newDef <- Deferred[F, O]
        _ <- X._2.offer(newDef)
        _ <- X._1.send(value)
      yield newDef.get
    
      def sendTo_(key: K, value: V, pipe: Pipe[F, V, O]): F[F[O]] = ???

  }

  case class ResultStream[F[_], I, V, O](
      ch: Channel[F, V],
      defMap: Map[I, Queue[F, Deferred[F, O]]]
  )
