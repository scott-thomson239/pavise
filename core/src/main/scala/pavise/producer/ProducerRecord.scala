package pavise.producer

import scodec.bits.ByteVector
import pavise.Header

case class ProducerRecord[K, V](topic: String, key: K, value: V, headers: List[Header])
