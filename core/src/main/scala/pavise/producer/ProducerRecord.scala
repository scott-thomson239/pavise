package pavise.producer

case class ProducerRecord[K, V](topic: String, key: K, value: V, headers: List[String])
