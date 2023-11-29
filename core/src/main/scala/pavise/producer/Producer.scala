package pavise.producer

import cats.syntax.all.*
import cats.effect.kernel.Resource
import pavise.KafkaClient
import pavise.producer.internal.BatchSender
import cats.effect.kernel.Async
import com.comcast.ip4s.IpAddress
import pavise.Metadata
import pavise.producer.internal.RecordBatcher
import fs2.io.net.Network
import pavise.*
import com.comcast.ip4s.Host
import com.comcast.ip4s.SocketAddress

trait Producer[F[_], K, V]:
  def produce(record: ProducerRecord[K, V]): F[F[RecordMetadata]]

object Producer:
  def resource[F[_]: Async: Network, K, V](
      settings: ProducerSettings[F, K, V]
  ): Resource[F, Producer[F, K, V]] = for
    bootstrapServers <- Resource.eval[F, List[SocketAddress[Host]]](
      settings.bootstrapServers
        .traverse(addr => SocketAddress.fromString(addr))
        .liftTo[F](new Exception("bad addrs"))
    )
    clusterState <- Resource.eval(ClusterState[F])
    metadata <- Metadata.resource(bootstrapServers, settings.metadataMaxAge, settings.metadataMaxIdle, clusterState)
    client <- KafkaClient.resource[F](
      metadata,
      settings.clientId,
      settings.requestTimeout,
      settings.connectionsMaxIdle,
      settings.reconnectBackoff,
      settings.reconnectBackoffMax,
      settings.socketConnectionSetupTimeout,
      settings.socketConnectionSetupTimeoutMax
    )
    _ <- Resource.eval(MetadataUpdater(client, clusterState))
    keySerializer <- settings.keySerializer
    valueSerializer <- settings.valueSerializer
    batchSender <- BatchSender.resource[F](
      settings.maxInFlightRequestsPerConnection,
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
    )(using Async[F], keySerializer, valueSerializer)
  yield new Producer[F, K, V]:
    def produce(record: ProducerRecord[K, V]): F[F[RecordMetadata]] =
      for // TODO: add option to fetch all cluster topics at once
        partition <- settings.partitioner.partition(record.topic, record.key, metadata)
        recordMetadataF <- recordBatcher.batch(record, partition)
      yield recordMetadataF
