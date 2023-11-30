package pavise

import cats.effect.kernel.Resource
import cats.effect.std.MapRef
import fs2.concurrent.Channel
import cats.effect.kernel.Async
import cats.effect.std.Supervisor
import fs2.*
import cats.syntax.all.*
import cats.effect.syntax.all.*
import cats.effect.std.Queue
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref

trait KeyedResultStream[F[_], K, I, V, O]:
  def sendTo(key: K, id: I, value: V, pipe: Pipe[F, V, (I, O)]): F[F[O]]

object KeyedResultStream:

  extension [F[_], K, V, O](s: KeyedResultStream[F, K, K, V, O])
    def sendTo_(key: K, value: V, pipe: Pipe[F, V, O]): F[F[O]] =
      s.sendTo(key, key, value, pipe.andThen(s => s.map(out => (key, out))))

  def resource[F[_]: Async, K, I, V, O](): Resource[F, KeyedResultStream[F, K, I, V, O]] = (
    Resource.eval(MapRef.inConcurrentHashMap[F, F, K, ResultStream[F, I, V, O]]()),
    Supervisor[F](await = true)
  ).mapN { (rStreamMap, supervisor) =>
    new KeyedResultStream[F, K, I, V, O]:
      def sendTo(key: K, id: I, value: V, pipe: Pipe[F, V, (I, O)]): F[F[O]] = for
        (ch, defQueue) <- getOrCreateResStream(id, supervisor, rStreamMap(key), pipe)
        newDef <- Deferred[F, O]
        _ <- defQueue.offer(newDef)
        _ <- ch.send(value)
      yield newDef.get
  }

  def getOrCreateResStream[F[_]: Async, I, V, O](
      id: I,
      supervisor: Supervisor[F],
      rStreamRef: Ref[F, Option[ResultStream[F, I, V, O]]],
      pipe: Pipe[F, V, (I, O)]
  ): F[(Channel[F, V], Queue[F, Deferred[F, O]])] =
    (Channel.unbounded[F, V], Queue.unbounded[F, Deferred[F, O]]).flatMapN { (newCh, newDefQueue) =>
      val newResStream = ResultStream(newCh, Map(id -> newDefQueue))
      rStreamRef
        .modify { rStreamOpt =>
          rStreamOpt
            .map { rStream =>
              rStream.defMap
                .get(id)
                .map(defQueue => (rStream.some, (rStream.ch, defQueue).some))
                .getOrElse(
                  (
                    rStream.copy(defMap = rStream.defMap.updated(id, newDefQueue)).some,
                    (rStream.ch, newDefQueue).some
                  )
                )
            }
            .getOrElse((newResStream.some, None))
        }
        .flatMap(
          _.fold(
            initResStream(newResStream, newDefQueue, supervisor, pipe).as((newCh, newDefQueue))
          )(_.pure[F])
        )
    }

  def initResStream[F[_]: Async, I, V, O](
      resStream: ResultStream[F, I, V, O],
      defQueue: Queue[F, Deferred[F, O]],
      supervisor: Supervisor[F],
      pipe: Pipe[F, V, (I, O)]
  ): F[Unit] =
    supervisor.supervise(
      resStream.ch.stream
        .through(pipe)
        .evalMap { case (id, res) =>
          resStream.defMap
            .get(id)
            .flatTraverse(_.tryTake)
            .flatMap { defOpt =>
              defOpt
                .liftTo[F](new Exception("out of order elements"))
                .flatMap(_.complete(res))
            }
        }
        .compile
        .drain
    ).void

  case class ResultStream[F[_], I, V, O](
      ch: Channel[F, V],
      defMap: Map[I, Queue[F, Deferred[F, O]]]
  )
