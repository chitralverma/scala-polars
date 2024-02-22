package org.polars.scala.polars.config

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import scala.jdk.CollectionConverters._

import org.polars.scala.polars.internal.jni.common
import org.polars.scala.polars.objectMapper

private case class ConfigExport(
    environment: Map[String, String],
    local: Map[String, String] = Map.empty[String, String]
)

class Config private (val options: Map[String, String]) {

  class ConfigUpdateBuilder private[config] () {
    private[this] val options = new java.util.HashMap[String, String]()

    /** Sets a configs from a Java Map.
      *
      * For more details, see
      * [[https://pola-rs.github.io/polars/py-polars/html/reference/config.html this.]]
      */
    def withOptions(opts: java.util.Map[String, String]): ConfigUpdateBuilder = synchronized {
      withOptions(opts.asScala)

      this
    }

    /** Sets a configs from an Iterable of key and value pairs.
      *
      * For more details, see
      * [[https://pola-rs.github.io/polars/py-polars/html/reference/config.html this.]]
      */
    def withOptions(opts: Iterable[(String, String)]): ConfigUpdateBuilder = synchronized {
      opts.foreach { case (key, value) => withOption(key, value) }

      this
    }

    /** Sets a config option from a key and value pair.
      *
      * For more details, see
      * [[https://pola-rs.github.io/polars/py-polars/html/reference/config.html this]] and
      * [[https://github.com/pola-rs/polars/blob/d3f4d63d6fcd02e4bddb445dc24ad8533f8b069d/py-polars/polars/config.py#L24 this]].
      */
    def withOption(key: String, value: String): ConfigUpdateBuilder = synchronized {
      (key, value) match {
        case (_, null) | (null, _) | (null, null) =>
          throw new IllegalArgumentException("Config key or value cannot be null or empty.")

        case (k, v) =>
          options.put(k.trim, v.trim)
          this
      }
    }

    /** Sets a configs from an existing file. */
    def fromPath(path: String): ConfigUpdateBuilder = synchronized {
      val configFile = new File(path)

      if (!configFile.exists() || !configFile.isFile)
        throw new IllegalArgumentException("Provided path must point to an existing file.")

      fromPath(configFile)
    }

    /** Sets a configs from an existing file. */
    def fromPath(file: File): ConfigUpdateBuilder = synchronized {
      val content: String =
        new String(Files.readAllBytes(Paths.get(file.toURI)), StandardCharsets.UTF_8)

      fromString(content)
    }

    /** Sets a configs from a JSON config string. */
    def fromString(configStr: String): ConfigUpdateBuilder = synchronized {
      val config = objectMapper.readValue(configStr, classOf[ConfigExport])

      withOptions(config.environment)
      this
    }

    /** Set table formatting style.
      *
      * For more details, see
      * [[https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.Config.set_tbl_formatting.html this.]]
      */
    def withTableFormatting(format: TableFormats.TableFormat): ConfigUpdateBuilder =
      synchronized {
        options.put("POLARS_FMT_TABLE_FORMATTING", format.toString)
        this
      }

    /** Set the max number of columns used to print tables.
      *
      * If n < 0, then print all the columns.
      *
      * For more details, see
      * [[https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.Config.set_tbl_cols.html this.]]
      */
    def withMaxTableColumns(nCols: Int): ConfigUpdateBuilder = synchronized {
      options.put("POLARS_FMT_MAX_COLS", nCols.toString)
      this
    }

    /** Set the max number of rows used to print tables.
      *
      * If n < 0, then print all the rows.
      *
      * For more details, see
      * [[https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.Config.set_tbl_rows.html this.]]
      */
    def withMaxTableRows(nRows: Int): ConfigUpdateBuilder = synchronized {
      options.put("POLARS_FMT_MAX_ROWS", nRows.toString)
      this
    }

    /** Print the dataframe shape below the dataframe when displaying tables.
      *
      * For more details, see
      * [[https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.Config.set_tbl_dataframe_shape_below.html this.]]
      */
    def withDataFrameShapeBelow(active: Boolean): ConfigUpdateBuilder = synchronized {
      options.put("POLARS_FMT_TABLE_DATAFRAME_SHAPE_BELOW", if (active) "1" else "0")
      this
    }

    /** Clear the current state of config. */
    def reset(): ConfigUpdateBuilder = {
      options.clear()
      this
    }

    /** Applies current configuration in a persistent way. */
    def apply(): Boolean = synchronized {
      Config.updateConfig(new Config(options.asScala.toMap))
      common.setConfigs(options)
    }
  }

  /** Creates a builder for Polars [[Config]]. */
  def update(): ConfigUpdateBuilder = new ConfigUpdateBuilder()

  /** Save the config to a specified path as a JSON config string. */
  def saveTo(path: String, overwrite: Boolean): Unit = {
    val configFile = new File(path)

    saveTo(configFile, overwrite)
  }

  /** Save the config to a specified path as a JSON config string. */
  def saveTo(path: File, overwrite: Boolean): Unit = synchronized {
    val configStr = this.toString

    if (path.exists() && path.isDirectory)
      throw new IllegalArgumentException("Provided path points to an existing directory.")

    val openOption =
      if (overwrite) Nil else Seq(StandardOpenOption.CREATE_NEW)

    Files.write(
      Paths.get(path.toURI),
      s"$configStr\n".getBytes(StandardCharsets.UTF_8),
      openOption: _*
    )
  }

  override def toString: String =
    objectMapper.writeValueAsString(ConfigExport(environment = options))
}

object Config {

  private var _instance: Config = _

  private[polars] def updateConfig(config: Config): Unit = synchronized {
    _instance = config
  }

  private[polars] def getConfig: Config = synchronized {
    Option(_instance) match {
      case None =>
        _instance = new Config(Map.empty[String, String])

      case _ =>
    }

    _instance
  }
}
