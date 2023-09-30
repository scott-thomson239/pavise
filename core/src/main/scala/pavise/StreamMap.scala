package pavise

import fs2.concurrent.Channel

trait StreamMap[F[_], K, V]:
    def get(key: K): F[Channel[F, V]]

object StreamMap:
    def create[F[_], K, V] = ???
