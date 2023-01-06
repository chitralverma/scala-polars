package org.polars.scala.polars.config

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import scala.jdk.CollectionConverters._

import org.json4s.native.Serialization.{read, write}
import org.polars.scala.polars.internal.jni

private case class ConfigExport(
    environment: Map[String, String],
    local: Map[String, String] = Map.empty[String, String]
)

class Config private (val options: Map[String, String]) {

  // noinspection TypeAnnotation
  private implicit val formats = org.json4s.DefaultFormats

  class Updater private[config] () {
    private[this] val options = new java.util.HashMap[String, String]()

    /** Sets a configs from an Iterable of key and value pairs.
      *
      * For more details, see
      * [[https://pola-rs.github.io/polars/py-polars/html/reference/config.html this.]]
      */
    def withOptions(opts: Iterable[(String, String)]): Updater = synchronized {
      opts.foreach { case (key, value) => withOption(key, value) }

      this
    }

    /** Sets a config option.
      *
      * For more details, see
      * [[https://pola-rs.github.io/polars/py-polars/html/reference/config.html this.]]
      */
    def withOption(key: String, value: String): Updater = synchronized {
      (key, value) match {
        case (_, null) | (null, _) | (null, null) =>
          throw new IllegalArgumentException("Config key or value cannot be null or empty.")

        case (k, v) =>
          options.put(k.trim, v.trim)
          this
      }
    }

    /** Sets a configs from an existing file. */
    def fromPath(path: String): Updater = synchronized {
      val configFile = new File(path)

      if (!configFile.exists() || !configFile.isFile)
        throw new IllegalArgumentException("Provided path must point to an existing file.")

      fromPath(configFile)
    }

    /** Sets a configs from an existing file. */
    def fromPath(file: File): Updater = synchronized {
      val content: String =
        new String(Files.readAllBytes(Paths.get(file.toURI)), StandardCharsets.UTF_8)

      fromString(content)
    }

    /** Sets a configs from a JSON config string. */
    def fromString(configStr: String): Updater = synchronized {
      val config = read[ConfigExport](configStr)

      withOptions(config.environment)
      this
    }

    /** Set table formatting style.
      *
      * For more details, see
      * [[https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.Config.set_tbl_formatting.html this.]]
      */
    def withTableFormatting(format: TableFormats.TableFormat): Updater = synchronized {
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
    def withMaxTableColumns(nCols: Int): Updater = synchronized {
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
    def withMaxTableRows(nRows: Int): Updater = synchronized {
      options.put("POLARS_FMT_MAX_ROWS", nRows.toString)
      this
    }

    /** Print the dataframe shape below the dataframe when displaying tables.
      *
      * For more details, see
      * [[https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.Config.set_tbl_dataframe_shape_below.html this.]]
      */
    def withDataFrameShapeBelow(active: Boolean): Updater = synchronized {
      options.put("POLARS_FMT_TABLE_DATAFRAME_SHAPE_BELOW", if (active) "1" else "0")
      this
    }

    def apply(): Boolean = synchronized {
      Config.updateConfig(new Config(options.asScala.toMap))
      jni.config._setConfigs(options)
    }
  }

  /** Creates a builder for Polars [[Config]]. */
  def update(): Updater = new Updater()

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

  override def toString: String = write(ConfigExport(environment = options))
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
