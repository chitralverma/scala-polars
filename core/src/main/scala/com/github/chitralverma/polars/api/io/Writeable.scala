package com.github.chitralverma.polars.api.io

import scala.collection.mutable.{Map => MutableMap}
import scala.jdk.CollectionConverters._

import com.github.chitralverma.polars.api.DataFrame
import com.github.chitralverma.polars.internal.jni.io.write._

/** Interface used to write a [[DataFrame]] in various formats to local filesystems and cloud
  * object stores (aws, gcp and azure). Use [[DataFrame.write write()]] to access this.
  *
  * Cloud options are global and can be set by methods like [[option option[s]()]]
  *   - For amazon s3 options, see
  *     [[https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html#variants here]]
  *   - For google cloud options, see
  *     [[https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html#variants here]]
  *   - For azure options, see
  *     [[https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants here]]
  *
  * This interface also supports the following global options,
  *   - `write_mode`: Specifies the behavior when data already exists at provided path. Supported
  *     values 'overwrite', 'error'. Default: error.
  *     - overwrite: Overwrites the existing data at the provided location.
  *     - error: Throw an exception if data already exists at the provided location.
  */
class Writeable private[polars] (ptr: Long) {
  import com.github.chitralverma.polars.jsonMapper

  private val _options: MutableMap[String, String] = MutableMap("write_mode" -> "error")

  /** Adds options for the underlying output format. */
  def options(opts: Iterable[(String, String)]): Writeable = synchronized {
    opts.foreach { case (key, value) => option(key, value) }
    this
  }

  /** Adds options for the underlying output format. */
  def options(opts: java.util.Map[String, String]): Writeable = synchronized {
    opts.asScala.foreach { case (key, value) => option(key, value) }
    this
  }

  /** Adds an option for the underlying output format. */
  def option(key: String, value: String): Writeable = synchronized {
    if (Option(key).exists(_.trim.isEmpty) || Option(value).exists(_.trim.isEmpty)) {
      throw new IllegalArgumentException("Option key or value cannot be null or empty.")
    }

    _options.put(key.trim, value.trim)
    this
  }

  /** Saves the content of the [[DataFrame]] in Parquet format at the specified path (local and
    * cloud).
    *
    * Supported options:
    *   - `write_parquet_parallel`: Serializes columns in parallel. Default: true.
    *   - `write_parquet_data_page_size`: Sets the maximum bytes size of a data page. Default:
    *     1024^2^ bytes.
    *   - `write_parquet_row_group_size`: Sets the row group size (in number of rows) during
    *     writing. This can reduce memory pressure and improve writing performance. Default:
    *     512^2^ rows.
    *   - `write_compression`: Sets the compression codec used for pages, for more compatibility
    *     guarantees, consider using Snappy. Supported values 'uncompressed', 'snappy', 'gzip',
    *     'brotli', 'lz4', 'zstd'. Default: zstd.
    *   - `write_compression_level`: Sets a valid level for codecs like 'gzip', 'brotli', 'zstd'.
    *     Defaults to compression default.
    *   - `write_parquet_stats`: Allows computation and writing of column statistics. Supported
    *     values 'full', 'none', 'some'. Default: some
    *
    * @param filePath
    *   output file location
    */
  def parquet(filePath: String): Unit =
    writeParquet(
      ptr = ptr,
      filePath = filePath,
      options = jsonMapper.writeValueAsString(_options)
    )

  /** Saves the content of the [[DataFrame]] in IPC format at the specified path (local and
    * cloud).
    *
    * Supported options:
    *   - `write_ipc_compat_level`: Sets compatibility. Supported values 'oldest', 'newest'.
    *     Default: newest.
    *   - `write_compression`: Sets the compression codec used for pages. Supported values
    *     'uncompressed', 'lz4', 'zstd'. Default: zstd.
    *   - `write_compression_level`: Sets a valid level for codecs like 'zstd'.
    *     Defaults to compression default.
    *
    * @param filePath
    *   output file location
    */
  def ipc(filePath: String): Unit =
    writeIPC(
      ptr = ptr,
      filePath = filePath,
      options = jsonMapper.writeValueAsString(_options)
    )

  /** Saves the content of the [[DataFrame]] in Avro format at the specified path (local and
    * cloud).
    *
    * Supported options:
    *   - `write_avro_record_name`: Sets the name of avro record. Default: "".
    *   - `write_compression`: Sets the compression codec used for blocks. Supported values
    *     'uncompressed', 'deflate', 'snappy'. Default: uncompressed.
    *
    * @param filePath
    *   output file location
    */
  def avro(filePath: String): Unit =
    writeAvro(
      ptr = ptr,
      filePath = filePath,
      options = jsonMapper.writeValueAsString(_options)
    )

  /** Saves the content of the [[DataFrame]] in CSV format at the specified path (local and
    * cloud).
    *
    * Supported options:
    *   - `write_csv_include_bom`: Sets whether to include UTF-8 Byte Order Mark (BOM) in the CSV
    *     output. Default: `false`.
    *   - `write_csv_include_header`: Sets whether to include header in the CSV output. Default:
    *     `true`.
    *   - `write_csv_float_scientific`: Sets whether to use scientific form always (true), never
    *     (false), or automatically (if not set) for `Float` and `Double` datatypes.
    *   - `write_csv_float_precision`: Sets the number of decimal places to write for `Float` and
    *     `Double` datatypes.
    *   - `write_csv_separator`: Sets the CSV file's column separator, defaulting to `,`
    *     character.
    *   - `write_csv_quote_char`: Sets the single byte character used for quoting, defaulting to
    *     `"` character.
    *   - `write_csv_date_format`: Sets the CSV file's date format defined by
    *     [[https://docs.rs/chrono/latest/chrono/format/strftime/index.html chrono]]. If no format
    *     specified, the default fractional-second precision is inferred from the maximum timeunit
    *     found in the frame's Datetime cols (if any).
    *   - `write_csv_time_format`: Sets the CSV file's time format defined by
    *     [[https://docs.rs/chrono/latest/chrono/format/strftime/index.html chrono]].
    *   - `write_csv_datetime_format`: Sets the CSV file's datetime format defined by
    *     [[https://docs.rs/chrono/latest/chrono/format/strftime/index.html chrono]].
    *   - `write_csv_line_terminator`: Sets the CSV file's line terminator. Default: "\n".
    *   - `write_csv_null_value`: Sets the CSV file's null value representation defaulting to the
    *     empty string.
    *   - `write_csv_quote_style`: Sets the CSV file's quoting style which indicates when to
    *     insert quotes around a field. Supported values 'necessary', 'always', 'non_numeric',
    *     'never'.
    *     - necessary (default): This puts quotes around fields only when necessary. They are
    *       necessary when fields contain a quote, separator or record terminator. Quotes are also
    *       necessary when writing an empty record (which is indistinguishable from a record with
    *       one empty field).
    *     - always: This puts quotes around every field. Always.
    *     - never: This never puts quotes around fields, even if that results in invalid CSV data
    *       (e.g.: by not quoting strings containing the separator).
    *     - non_numeric: This puts quotes around all fields that are non-numeric. Namely, when
    *       writing a field that does not parse as a valid float or integer, then quotes will be
    *       used even if they aren't strictly necessary.
    *
    * @note
    *   compression is not supported for this format.
    * @param filePath
    *   output file location
    */
  def csv(filePath: String): Unit =
    writeCSV(
      ptr = ptr,
      filePath = filePath,
      options = jsonMapper.writeValueAsString(_options)
    )

  /** Saves the content of the [[DataFrame]] in JSON format at the specified path (local and
    * cloud).
    *
    * A single JSON array containing each DataFrame row as an object. The length of the array is
    * the number of rows in the DataFrame. Use this to create valid JSON that can be deserialized
    * back into an array in one fell swoop.
    *
    * @note
    *   compression is not supported for this format.
    *
    * @param filePath
    *   output file location
    */
  def json(filePath: String): Unit = {
    option("write_json_format", "json")
    writeJson(
      ptr = ptr,
      filePath = filePath,
      options = jsonMapper.writeValueAsString(_options)
    )
  }

  /** Saves the content of the [[DataFrame]] in Newline Delimited JSON (ndjson) format at the
    * specified path (local and cloud).
    *
    * Each DataFrame row is serialized as a JSON object on a separate line. The number of lines in
    * the output is the number of rows in the DataFrame.
    *
    * The [[https://pola-rs.github.io/polars/py-polars/html/reference/config.html JSON Lines]]
    * format makes it easy to read records in a streaming fashion, one (line) at a time. But the
    * output in its entirety is not valid JSON; only the individual lines are. It is recommended
    * to use the file extension `.jsonl` when saving as JSON Lines.
    *
    * @note
    *   compression is not supported for this format.
    * @param filePath
    *   output file location
    */
  def jsonLines(filePath: String): Unit = {
    option("write_json_format", "json_lines")
    writeJson(
      ptr = ptr,
      filePath = filePath,
      options = jsonMapper.writeValueAsString(_options)
    )
  }
}
