package pavise

import scala.concurrent.duration.FiniteDuration

object Timed:
  opaque type Timed[F[_], A] = (FiniteDuration, F[A])

  def start[F[_], A](timeout: FiniteDuration): Timed[F, A] = ???

  // extension [F[_], A] (t: Timed[F, A])
  //   def value: F[A] = t
