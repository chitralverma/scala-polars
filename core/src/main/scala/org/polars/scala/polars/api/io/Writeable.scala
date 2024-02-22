package org.polars.scala.polars.api.io

import java.util.Locale

import scala.collection.mutable.{Map => MutableMap}
import scala.jdk.CollectionConverters._

import org.polars.scala.polars.internal.jni.io.write._

object WriteCompressions extends Enumeration {
  type WriteCompression = Value

  private lazy val stringMap: Map[String, WriteCompression] =
    values.map(v => (v.toString.toLowerCase(Locale.ROOT), v)).toMap

  def fromString(str: String): Option[WriteCompression] =
    stringMap.get(str.toLowerCase(Locale.ROOT))

  val lz4, uncompressed, snappy, gzip, lzo, brotli, zstd, deflate = Value
}

object WriteModes extends Enumeration {
  type WriteMode = Value

  private lazy val stringMap: Map[String, WriteMode] =
    values.map(v => (v.toString.toLowerCase(Locale.ROOT), v)).toMap

  def fromString(str: String): Option[WriteMode] =
    stringMap.get(str.toLowerCase(Locale.ROOT))

  val ErrorIfExists, Overwrite = Value
}

class Writeable private[polars] (ptr: Long) {
  import org.polars.scala.polars.objectMapper

  private var _mode: String = WriteModes.ErrorIfExists.toString
  private var _compression: String = WriteCompressions.zstd.toString
  private var _compressionLevel: Int = -1
  private val _options: MutableMap[String, String] = MutableMap.empty

  def compression(
      value: WriteCompressions.WriteCompression,
      level: Option[Int]
  ): Writeable = synchronized {
    _compression = value.toString
    level match {
      case Some(value) => _compressionLevel = value
      case None =>
    }

    this
  }

  def compression(
      value: String,
      level: Option[Int] = None
  ): Writeable = synchronized {
    compression(
      WriteCompressions
        .fromString(value)
        .getOrElse(
          throw new IllegalArgumentException(
            s"Provided value '$value' is not a valid write mode."
          )
        ),
      level
    )
  }

  def mode(value: WriteModes.WriteMode): Writeable = synchronized {
    _mode = value.toString
    this
  }

  def mode(value: String): Writeable = synchronized {
    mode(
      WriteModes
        .fromString(value)
        .getOrElse(
          throw new IllegalArgumentException(
            s"Provided value '$value' is not a valid write mode."
          )
        )
    )
  }

  def withOption(key: String, value: String): Writeable = synchronized {
    (key, value) match {
      case (_, null) | (null, _) | (null, null) =>
        throw new IllegalArgumentException("Option key or value cannot be null or empty.")

      case (k, v) =>
        _options.put(k.trim, v.trim)
        this
    }
  }

  def options(opts: java.util.Map[String, String]): Writeable = synchronized {
    opts.asScala.foreach { case (key, value) => withOption(key, value) }

    this
  }

  def options(opts: Iterable[(String, String)]): Writeable = synchronized {
    opts.foreach { case (key, value) => withOption(key, value) }

    this
  }

  def parquet(filePath: String, writeStats: Boolean = false): Unit =
    writeParquet(
      ptr = ptr,
      filePath = filePath,
      writeStats = writeStats,
      compression = _compression,
      compressionLevel = _compressionLevel,
      options = objectMapper.writeValueAsString(_options),
      writeMode = _mode
    )

  def ipc(filePath: String): Unit = {
    WriteCompressions.fromString(_compression).get match {
      case WriteCompressions.zstd | WriteCompressions.uncompressed | WriteCompressions.lz4 =>
      case v =>
        throw new IllegalArgumentException(
          s"Compression for IPC format must be one of {{'uncompressed', 'lz4', 'zstd'}}, got $v"
        )
    }

    writeIPC(
      ptr = ptr,
      filePath = filePath,
      compression = _compression,
      options = objectMapper.writeValueAsString(_options),
      writeMode = _mode
    )
  }

  def avro(filePath: String): Unit = {
    WriteCompressions.fromString(_compression).get match {
      case WriteCompressions.uncompressed | WriteCompressions.deflate |
          WriteCompressions.snappy =>
      case v =>
        throw new IllegalArgumentException(
          s"Compression for Avro format must be one of {{'uncompressed', 'deflate', 'snappy'}}, got $v"
        )
    }

    writeAvro(
      ptr = ptr,
      filePath = filePath,
      compression = _compression,
      options = objectMapper.writeValueAsString(_options),
      writeMode = _mode
    )
  }
}
