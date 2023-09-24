package pavise.producer

import scala.concurrent.duration.*
import pavise.*
import cats.syntax.all.*
import cats.effect.kernel.Resource

case class ProducerSettings[F[_], K, V](
    keySerializer: Resource[F, KeySerializer[F, K]],
    valueSerializer: Resource[F, ValueSerializer[F, V]],
    bootstrapServers: List[String],
    acks: Acks,
    batchSize: Int,
    clientId: String,
    retries: Int,
    maxInFlightRequestsPerConnection: Int,
    idempotenceEnabled: Boolean,
    linger: FiniteDuration,
    requestTimeout: FiniteDuration,
    deliveryTimeout: FiniteDuration,
    closeTimeout: FiniteDuration,
    partitioner: Partitioner[F, K],
    retryBackOff: FiniteDuration,
    maxRequestSize: Int,
    bufferMemory: Int,
    compressionType: CompressionType,
    maxBlockTime: FiniteDuration,
    partitionerAvailabilityTimeout: FiniteDuration,
    resolveCanonicalBootstrapServers: Boolean,
    metadataMaxAge: FiniteDuration,
    metadataMaxIdle: FiniteDuration,
    connectionsMaxIdle: FiniteDuration,
    reconnectBackoff: FiniteDuration,
    reconnectBackoffMax: FiniteDuration,
    sendBuffer: Int,
    receiveBuffer: Int,
    socketConnectionSetupTimeout: FiniteDuration,
    socketConnectionSetupTimeoutMax: FiniteDuration
):
  def withBootstrapServers(
      bootstrapServers: List[String]
  ): ProducerSettings[F, K, V] = copy(bootstrapServers = bootstrapServers)

  def withAcks(acks: Acks): ProducerSettings[F, K, V] = copy(acks = acks)

  def withBatchSize(batchSize: Int): ProducerSettings[F, K, V] =
    copy(batchSize = (batchSize, 1).maximum)

  def withClientId(clientId: String): ProducerSettings[F, K, V] =
    copy(clientId = clientId)

  def withRetries(retries: Int): ProducerSettings[F, K, V] =
    copy(retries = retries)

  def withMaxInFlightRequestsPerConnection(
      maxInFlightRequestsPerConnection: Int
  ): ProducerSettings[F, K, V] =
    copy(maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection)

  def withEnableIdempotence(
      enableIdempotence: Boolean
  ): ProducerSettings[F, K, V] = copy(idempotenceEnabled = enableIdempotence)

  def withLinger(linger: FiniteDuration): ProducerSettings[F, K, V] =
    copy(linger = linger)

  def withRequestTimeout(
      requestTimeout: FiniteDuration
  ): ProducerSettings[F, K, V] = copy(requestTimeout = requestTimeout)

  def withDeliveryTimeout(
      deliveryTimeout: FiniteDuration
  ): ProducerSettings[F, K, V] = copy(deliveryTimeout = deliveryTimeout)

  def withCloseTimeout(
      closeTimeout: FiniteDuration
  ): ProducerSettings[F, K, V] = copy(closeTimeout = closeTimeout)

object ProducerSettings:

  def apply[F[_], K, V](
      keySerializer: Resource[F, KeySerializer[F, K]],
      valueSerializer: Resource[F, ValueSerializer[F, V]]
  ): ProducerSettings[F, K, V] =
    ProducerSettings(
      keySerializer,
      valueSerializer,
      bootstrapServers = List(""),
      acks = Acks.All,
      batchSize = 16384,
      clientId = "",
      retries = 0,
      maxInFlightRequestsPerConnection = 5,
      idempotenceEnabled = false,
      linger = 0.seconds,
      requestTimeout = 30.seconds,
      deliveryTimeout = 2.minutes,
      closeTimeout = 60.seconds,
      partitioner = Partitioner.stickyPartitioner[F, K],
      retryBackOff = 100.millis,
      maxRequestSize = 1048576,
      bufferMemory = 33554432,
      compressionType = CompressionType.None,
      maxBlockTime = 1.minute,
      partitionerAvailabilityTimeout = 0.seconds,
      resolveCanonicalBootstrapServers = false,
      metadataMaxAge = 5.minutes,
      metadataMaxIdle = 5.minutes,
      connectionsMaxIdle = 9.minutes,
      reconnectBackoff = 50.millis,
      reconnectBackoffMax = 1.second,
      sendBuffer = 131072,
      receiveBuffer = 32768,
      socketConnectionSetupTimeout = 10.seconds,
      socketConnectionSetupTimeoutMax = 30.seconds
    )
