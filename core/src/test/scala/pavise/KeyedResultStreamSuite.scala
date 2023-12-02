package pavise

import munit.CatsEffectSuite
import cats.effect.IO
import cats.effect.kernel.Ref
import fs2.*
import cats.syntax.all.*

class KeyedResultStreamSuite extends CatsEffectSuite:
  def krStreamFixture[K, I, V, O] =
    ResourceFunFixture(KeyedResultStream.resource[IO, K, I, V, O]())

  val counterPipe: Pipe[IO, Unit, Int] = in =>
    Stream.eval(Ref.of[IO, Int](0)).flatMap { counter =>
      in.evalMap { _ =>
        counter.updateAndGet(_ + 1)
      }
    }

  krStreamFixture[Int, Int, Unit, Int].test(
    "Messages of the same key are sent to the same stream"
  ) { krStream =>
    val sendToSameKey = krStream.sendTo_(0, (), counterPipe).flatten
    (sendToSameKey, sendToSameKey, sendToSameKey).mapN { (res0, res1, res2) =>
      assertEquals(res0, 1)
      assertEquals(res1, 2)
      assertEquals(res2, 3)
    }
  }

  krStreamFixture[Int, Int, Unit, Int].test(
    "Messages of different keys are sent to different streams"
  ) { krStream =>
    def sendToKey(key: Int): IO[Int] = krStream.sendTo_(key, (), counterPipe).flatten
    (sendToKey(0), sendToKey(1), sendToKey(2)).mapN { (res0, res1, res2) =>
      assertEquals(res0, 1)
      assertEquals(res1, 1)
      assertEquals(res2, 1)
    }
  }

  krStreamFixture[Int, Int, Int, Int].test(
    "Messages sent with an id still return the correct result if produced out of order"
  ) { krStream =>
    val reorderPipe: Pipe[IO, Int, (Int, Int)] = in =>
      in.chunkN(3, false).map(chunk => Chunk.array(chunk.map(i => (i, i)).toArray.reverse)).unchunks
    (1, 2, 3).traverse(i => krStream.sendTo(0, i, i, reorderPipe).flatten).map {
      (res0, res1, res2) =>
        assertEquals(res0, 1)
        assertEquals(res1, 2)
        assertEquals(res2, 3)
    }
  }

  krStreamFixture[Int, Int, Unit, Int].test(
    "Messages resulting in an error will only terminate the stream for that key"
  ) { krStream => 
    val explodingCounter = counterPipe.andThen { in => 
      in.evalMap { i => 
        if (i < 2) then
          i.pure[IO]
        else
          IO.raiseError(new Exception("boom"))
      }
    }
    val sendToSameKey = krStream.sendTo_(0, (), explodingCounter)
    (sendToSameKey, sendToSameKey, sendToSameKey).mapN { (res0F, res1F, res2F) => 
      assertIO(res0F, 1)
      interceptMessageIO[Exception]("boom")(res1F)
      assertIO(res2F, 1)
    }
  }
