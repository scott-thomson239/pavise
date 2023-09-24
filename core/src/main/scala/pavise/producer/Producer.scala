package pavise.producer

import cats.syntax.all.*
import cats.effect.kernel.Resource
import pavise.KafkaClient
import pavise.producer.internal.RecordAccumulator
import cats.effect.kernel.Async
import com.comcast.ip4s.IpAddress

trait Producer[F[_], K, V]:

  def produce(record: ProducerRecord[K, V]): F[F[RecordMetadata]]

object Producer:

  def resource[F[_]: Async, K, V](
      settings: ProducerSettings[F, K, V]
  ): Resource[F, Producer[F, K, V]] = for
    bootstrapServers <- Resource.eval[F, List[IpAddress]](
      settings.bootstrapServers
        .traverse(addr => IpAddress.fromString(addr))
        .liftTo[F](new Exception("bad addrs"))
    )
    client <- KafkaClient.resource[F](
      settings.clientId,
      bootstrapServers,
      settings.requestTimeout,
      settings.metadataMaxAge,
      settings.metadataMaxIdle,
      settings.connectionsMaxIdle,
      settings.reconnectBackoff,
      settings.reconnectBackoffMax,
      settings.socketConnectionSetupTimeout,
      settings.socketConnectionSetupTimeoutMax
    )
    keySerializer <- settings.keySerializer
    valueSerializer <- settings.valueSerializer
    recordAccumulator <- Resource.eval(
      RecordAccumulator.create[F, K, V](
        settings.batchSize,
        settings.compressionType,
        settings.linger,
        settings.retryBackOff,
        settings.deliveryTimeout,
        settings.maxBlockTime,
        settings.bufferMemory,
        client
      )
    )
  yield new Producer[F, K, V]:
    def produce(record: ProducerRecord[K, V]): F[F[RecordMetadata]] =
      settings.partitioner.partition(record.topic, record.key).flatMap { partition =>
        recordAccumulator.send(record, partition)
      }
