package pavise.producer

import cats.syntax.all.*
import cats.effect.kernel.Resource
import pavise.KafkaClient
import pavise.producer.internal.BatchSender
import cats.effect.kernel.Async
import com.comcast.ip4s.IpAddress
import pavise.Metadata
import pavise.producer.internal.RecordBatcher

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
      settings.connectionsMaxIdle,
      settings.reconnectBackoff,
      settings.reconnectBackoffMax,
      settings.socketConnectionSetupTimeout,
      settings.socketConnectionSetupTimeoutMax
    )
    metadata <- Resource.eval(
      Metadata.create(settings.metadataMaxAge, settings.metadataMaxIdle, client)
    )
    keySerializer <- settings.keySerializer
    valueSerializer <- settings.valueSerializer
    batchSender <- BatchSender.resource[F](
      settings.retryBackOff,
      settings.deliveryTimeout,
      settings.maxBlockTime,
      metadata,
      client
    )
    recordBatcher <- RecordBatcher.resource[F, K, V](
      settings.batchSize,
      settings.compressionType,
      settings.linger,
      settings.maxBlockTime,
      settings.bufferMemory,
      batchSender
    )
  yield new Producer[F, K, V]:
    def produce(record: ProducerRecord[K, V]): F[F[RecordMetadata]] = for // TODO: add option to fetch all cluster topics at once
      partition <- settings.partitioner.partition(record.topic, record.key, metadata)
      recordMetadataF <- recordBatcher.batch(record, partition)
    yield recordMetadataF
