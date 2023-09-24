package pavise

sealed trait CompressionType
object CompressionType:
  case object None extends CompressionType
  case object GZip extends CompressionType
  case object Snappy extends CompressionType
  case object Lz4 extends CompressionType
  case object Zstd extends CompressionType
