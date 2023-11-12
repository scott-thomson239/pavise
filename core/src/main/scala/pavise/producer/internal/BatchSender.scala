package pavise.producer.internal

import scala.concurrent.duration.*
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
import pavise.protocol.*
import fs2.*
import pavise.protocol.message.ProduceRequest
import cats.effect.std.Semaphore
import pavise.protocol.RecordBatch.Attributes
import pavise.producer.Acks
import pavise.protocol.message.ProduceRequest.TopicData
import pavise.protocol.message.ProduceRequest.PartitionData

trait BatchSender[F[_]]:
  def send(topicPartition: TopicPartition, batch: RecordBatch): F[F[RecordMetadata]]

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
      .resource[F, Int, TopicPartition, (TopicPartition, RecordBatch), RecordMetadata]()
      .map { keyResStream =>
        new BatchSender[F]:
          def send(topicPartition: TopicPartition, batch: RecordBatch): F[F[RecordMetadata]] =
            metadata.currentLeader(topicPartition).flatMap { leaderId =>
              val pipe
                  : Pipe[F, (TopicPartition, RecordBatch), (TopicPartition, RecordMetadata)] =
                in =>
                  Stream
                    .eval(Semaphore[F](maxInFlight))
                    .flatMap { sem =>
                      in.chunks
                        .map[ProduceRequest] { recordBytes =>
                          val topicData = recordBytes.toList
                            .groupBy(_._1.topic)
                            .map { case (topic, batches) =>
                              val partData = batches.map { case (topicPartition, batch) =>
                                PartitionData(topicPartition.partition, batch)
                              }
                              TopicData(topic, partData)
                            }
                            .toList // probably slow, fix later
                          ProduceRequest(None, Acks.All, deliveryTimeout, topicData)
                        }
                        .evalMap(req => sem.acquire *> client.sendRequest(leaderId, req))
                        .parEvalMapUnbounded(respF => respF <* sem.release) // retry here
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
