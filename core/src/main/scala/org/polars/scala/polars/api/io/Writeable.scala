package org.polars.scala.polars.api.io

import java.util.Locale

import scala.collection.mutable.{Map => MutableMap}
import scala.jdk.CollectionConverters._

import org.polars.scala.polars.internal.jni.io.write._

class Writeable private[polars] (ptr: Long) {
  import org.polars.scala.polars.jsonMapper

  private val _options: MutableMap[String, String] = MutableMap("write_mode" -> "errorifexists")

  def option(key: String, value: String): Writeable = synchronized {
    if (Option(key).exists(_.trim.isEmpty) || Option(value).exists(_.trim.isEmpty)) {
      throw new IllegalArgumentException("Option key or value cannot be null or empty.")
    }

    _options.put(key.trim, value.trim)
    this
  }

  def options(opts: java.util.Map[String, String]): Writeable = synchronized {
    opts.asScala.foreach { case (key, value) => option(key, value) }
    this
  }

  def options(opts: Iterable[(String, String)]): Writeable = synchronized {
    opts.foreach { case (key, value) => option(key, value) }
    this
  }

  def parquet(filePath: String): Unit =
    writeParquet(
      ptr = ptr,
      filePath = filePath,
      options = jsonMapper.writeValueAsString(_options)
    )

  def ipc(filePath: String): Unit =
    writeIPC(
      ptr = ptr,
      filePath = filePath,
      options = jsonMapper.writeValueAsString(_options)
    )

  def avro(filePath: String): Unit =
    writeAvro(
      ptr = ptr,
      filePath = filePath,
      options = jsonMapper.writeValueAsString(_options)
    )

  def csv(filePath: String): Unit =
    writeCSV(
      ptr = ptr,
      filePath = filePath,
      options = jsonMapper.writeValueAsString(_options)
    )

  def json(filePath: String): Unit = {
    option("write_json_format", "json")
    writeJson(
      ptr = ptr,
      filePath = filePath,
      options = jsonMapper.writeValueAsString(_options)
    )
  }

  def json_lines(filePath: String): Unit = {
    option("write_json_format", "json_lines")
    writeJson(
      ptr = ptr,
      filePath = filePath,
      options = jsonMapper.writeValueAsString(_options)
    )
  }
}
