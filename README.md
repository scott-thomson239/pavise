# Pavise

Pavise is a client library for [Apache Kafka](https://kafka.apache.org/), written in Scala 3. It implements the Kafka protocol from the ground up using scodec, cats-effect and fs2.

Thanks to not relying on the official Java client, Pavise is able to bring Kafka to all existing Scala platforms (JVM, JS and Native).

Currently, the library is heavily experimental and only supports the producer API at the moment.
