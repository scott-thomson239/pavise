package pavise.producer.internal

import pavise.producer.RecordMetadata
import pavise.producer.ProducerRecord
import scala.concurrent.duration.FiniteDuration
import pavise.CompressionType
import pavise.KafkaClient
import cats.effect.kernel.Resource
import cats.effect.std.MapRef
import cats.effect.kernel.Sync
import cats.syntax.all.*
import pavise.TopicPartition
import scodec.bits.*
import pavise.Metadata
import pavise.*
import cats.effect.kernel.Deferred
import cats.effect.kernel.Async
import pavise.protocol.message.ProduceResponse
import fs2.*
import pavise.protocol.message.ProduceRequest
import cats.effect.std.Semaphore

trait BatchSender[F[_]]:
  def send(topicPartition: TopicPartition, batch: ByteVector): F[F[RecordMetadata]]

object BatchSender:
  def resource[F[_]: Async](
      maxInFlight: Int,
      retryBackoff: FiniteDuration,
      deliveryTimeout: FiniteDuration,
      maxBlockTime: FiniteDuration,
      metadata: Metadata[F],
      client: KafkaClient[F]
  ): Resource[F, BatchSender[F]] =
    KeyedResultStream
      .resource[F, Int, TopicPartition, (TopicPartition, ByteVector), RecordMetadata]()
      .map { keyResStream =>
        new BatchSender[F]:
          def send(topicPartition: TopicPartition, batch: ByteVector): F[F[RecordMetadata]] =
            metadata.currentLeader(topicPartition).flatMap { leaderId =>
              val pipe
                  : Pipe[F, (TopicPartition, ByteVector), (TopicPartition, RecordMetadata)] =
                in =>
                  Stream
                    .eval(Semaphore[F](maxInFlight))
                    .flatMap { sem =>
                      in.chunks
                        .map[ProduceRequest](???)
                        .evalMap(req => sem.acquire *> client.sendRequest(leaderId, req))
                        .parEvalMapUnbounded(respF => respF <* sem.release) //retry here
                        .flatMap { resp =>
                          Stream.emits(resp.responses.flatMap { r =>
                            r.partitionResponses.map { pr =>
                              (TopicPartition(r.name, pr.index), RecordMetadata())
                            }
                          })
                        }
                    }
              keyResStream.sendTo(leaderId, topicPartition, (topicPartition, batch), pipe)
            }
      }
