package org.polars.scala.polars.api.io

import scala.annotation.varargs
import scala.collection.mutable.{Map => MutableMap}
import scala.jdk.CollectionConverters._

import org.polars.scala.polars.Polars
import org.polars.scala.polars.api.LazyFrame
import org.polars.scala.polars.internal.jni.io.scan._

/** Interface used to scan datasets of various formats from local filesystems and cloud object
  * stores (aws, gcp and azure). Use [[Polars.scan scan()]] to access this.
  *
  * Cloud options are global and can be set by methods like [[option option[s]()]]
  *   - For amazon s3 options, see
  *     [[https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html#variants here]]
  *   - For google cloud options, see
  *     [[https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html#variants here]]
  *   - For azure options, see
  *     [[https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants here]]
  */
class Scannable private[polars] () {
  import org.polars.scala.polars.jsonMapper

  private val _options: MutableMap[String, String] = MutableMap.empty[String, String]

  /** Adds options for the underlying dataset. */
  def options(opts: Iterable[(String, String)]): Scannable = synchronized {
    opts.foreach { case (key, value) => option(key, value) }
    this
  }

  /** Adds options for the underlying dataset. */
  def options(opts: java.util.Map[String, String]): Scannable = synchronized {
    opts.asScala.foreach { case (key, value) => option(key, value) }
    this
  }

  /** Adds an option for the underlying dataset. */
  def option(key: String, value: String): Scannable = synchronized {
    if (Option(key).exists(_.trim.isEmpty) || Option(value).exists(_.trim.isEmpty)) {
      throw new IllegalArgumentException("Option key or value cannot be null or empty.")
    }

    _options.put(key.trim, value.trim)
    this
  }

  /** Scans the contents of a dataset in Parquet format from the specified path(s) (local and
    * cloud). Provided paths support globbing and expansion.
    *
    * Supported options:
    *   - `scan_parquet_n_rows`: Sets the maximum rows to read from parquet files. All rows are
    *     scanned if not specified.
    *   - `scan_parquet_parallel`: This determines the direction and strategy of parallelism.
    *     Supported values ‘auto’, ‘columns’, ‘row_groups’, ‘prefiltered’, ‘none’.
    *     - auto (default): Automatically determine over which unit to parallelize.
    *     - columns: Parallelize over the columns.
    *     - row_groups: Parallelize over the row groups.
    *     - prefiltered: First evaluates the pushed-down predicates in parallel and determines a
    *       mask of which rows to read. Then, it parallelizes over both the columns and the row
    *       groups while filtering out rows that do not need to be read. This can provide
    *       significant speedups for large files (i.e. many row-groups) with a predicate that
    *       filters clustered rows or filters heavily. In other cases, prefiltered may slow down
    *       the scan compared other strategies. The prefiltered settings falls back to auto if no
    *       predicate is given.
    *     - none: Disables parallelism for scan.
    *   - `scan_parquet_row_index_name`: If set, this inserts a row index column with the given
    *     name. Default: `null`
    *   - `scan_parquet_row_index_offset`: Sets the offset (>=0) to start the row index column
    *     (only used if `scan_parquet_row_index_name` is set). Default: 0
    *   - `scan_parquet_use_statistics`: Use statistics in the parquet to determine if pages can
    *     be skipped from reading. Default: `true`.
    *   - `scan_parquet_cache`: Cache the result after scanning. Default: `true`.
    *   - `scan_parquet_glob`: Expand path given via globbing rules. Default: `true`.
    *   - `scan_parquet_low_memory`: Reduce memory pressure at the expense of performance.
    *     Default: `false`.
    *   - `scan_parquet_rechunk`: In case of reading multiple files via a glob pattern re-chunk
    *     the final DataFrame into contiguous memory chunks. Default: `false`.
    *   - `scan_parquet_allow_missing_columns`: When reading a list of parquet files, if a column
    *     existing in the first file cannot be found in subsequent files, the default behavior is
    *     to raise an error. However, if this option is set to `true`, a full-NULL column is
    *     returned instead of throwing error for the files that do not contain the column.
    *     Default: `false`.
    *   - `scan_parquet_include_file_paths`: If set, this option includes the path of the source
    *     file(s) as a column with provided name. Default: `null`
    *   - `scan_parquet_hive_scan_partitions`: Infer statistics and schema from hive partitions
    *     and use them to prune reads. Set as `false` to automatically enable for single directory
    *     scans. Default: `true`.
    *   - `scan_parquet_hive_try_parse_dates`: Whether to try parsing hive values as date/datetime
    *     types. Default: `true`.
    *
    * @param path
    *   input file location
    * @param paths
    *   additional input file locations
    *
    * @note
    *   If multiple paths are provided, connection options are inferred from the first path. So
    *   all provided paths must be of the same object store.
    */
  @varargs
  def parquet(path: String, paths: String*): LazyFrame =
    LazyFrame.withPtr(
      scanParquet(
        paths = paths.+:(path).toArray[String],
        options = jsonMapper.writeValueAsString(_options)
      )
    )

  /** Scans the contents of a dataset in IPC format from the specified path(s) (local and cloud).
    * Provided paths support globbing and expansion.
    *
    * Supported options:
    *   - `scan_ipc_n_rows`: Sets the maximum rows to read from ipc files. All rows are scanned if
    *     not specified.
    *   - `scan_ipc_cache`: Cache the result after scanning. Default: `true`.
    *   - `scan_ipc_rechunk`: In case of reading multiple files via a glob pattern re-chunk the
    *     final DataFrame into contiguous memory chunks. Default: `false`.
    *   - `scan_ipc_row_index_name`: If set, this inserts a row index column with the given name.
    *     Default: `null`
    *   - `scan_ipc_row_index_offset`: Sets the offset (>=0) to start the row index column (only
    *     used if `scan_ipc_row_index_name` is set). Default: 0
    *   - `scan_ipc_include_file_paths`: If set, this option includes the path of the source
    *     file(s) as a column with provided name. Default: `null`
    *   - `scan_ipc_hive_scan_partitions`: Infer statistics and schema from hive partitions and
    *     use them to prune reads. Set as `false` to automatically enable for single directory
    *     scans. Default: `true`.
    *   - `scan_ipc_hive_try_parse_dates`: Whether to try parsing hive values as date/datetime
    *     types. Default: `true`.
    *
    * @param path
    *   input file location
    * @param paths
    *   additional input file locations
    *
    * @note
    *   If multiple paths are provided, connection options are inferred from the first path. So
    *   all provided paths must be of the same object store.
    */
  @varargs
  def ipc(path: String, paths: String*): LazyFrame =
    LazyFrame.withPtr(
      scanIPC(
        paths = paths.+:(path).toArray[String],
        options = jsonMapper.writeValueAsString(_options)
      )
    )

  // /** Saves the content of the [[DataFrame]] in Avro format at the specified path (local and
  //   * cloud).
  //   *
  //   * Supported options:
  //   *   - `write_avro_record_name`: Sets the name of avro record. Default: "".
  //   *   - `write_compression`: Sets the compression codec used for blocks. Supported values
  //   *     'uncompressed', 'deflate', 'snappy'. Default: uncompressed.
  //   *
  //   * @param filePath
  //   *   output file location
  //   */
  // def avro(filePath: String): Unit =
  //   writeAvro(
  //     ptr = ptr,
  //     filePath = filePath,
  //     options = jsonMapper.writeValueAsString(_options)
  //   )

  // /** Saves the content of the [[DataFrame]] in CSV format at the specified path (local and
  //   * cloud).
  //   *
  //   * Supported options:
  //   *   - `write_csv_include_bom`: Sets whether to include UTF-8 Byte Order Mark (BOM) in the CSV
  //   *     output. Default: `false`.
  //   *   - `write_csv_include_header`: Sets whether to include header in the CSV output. Default:
  //   *     `true`.
  //   *   - `write_csv_float_scientific`: Sets whether to use scientific form always (true), never
  //   *     (false), or automatically (if not set) for `Float` and `Double` datatypes.
  //   *   - `write_csv_float_precision`: Sets the number of decimal places to write for `Float` and
  //   *     `Double` datatypes.
  //   *   - `write_csv_separator`: Sets the CSV file's column separator, defaulting to `,`
  //   *     character.
  //   *   - `write_csv_quote_char`: Sets the single byte character used for quoting, defaulting to
  //   *     `"` character.
  //   *   - `write_csv_date_format`: Sets the CSV file's date format defined by
  //   *     [[https://docs.rs/chrono/latest/chrono/format/strftime/index.html chrono]]. If no format
  //   *     specified, the default fractional-second precision is inferred from the maximum timeunit
  //   *     found in the frame's Datetime cols (if any).
  //   *   - `write_csv_time_format`: Sets the CSV file's time format defined by
  //   *     [[https://docs.rs/chrono/latest/chrono/format/strftime/index.html chrono]].
  //   *   - `write_csv_datetime_format`: Sets the CSV file's datetime format defined by
  //   *     [[https://docs.rs/chrono/latest/chrono/format/strftime/index.html chrono]].
  //   *   - `write_csv_line_terminator`: Sets the CSV file's line terminator. Default: "\n".
  //   *   - `write_csv_null_value`: Sets the CSV file's null value representation defaulting to the
  //   *     empty string.
  //   *   - `write_csv_quote_style`: Sets the CSV file's quoting style which indicates when to
  //   *     insert quotes around a field. Supported values 'necessary', 'always', 'non_numeric',
  //   *     'never'.
  //   *     - necessary (default): This puts quotes around fields only when necessary. They are
  //   *       necessary when fields contain a quote, separator or record terminator. Quotes are also
  //   *       necessary when writing an empty record (which is indistinguishable from a record with
  //   *       one empty field).
  //   *     - always: This puts quotes around every field. Always.
  //   *     - never: This never puts quotes around fields, even if that results in invalid CSV data
  //   *       (e.g.: by not quoting strings containing the separator).
  //   *     - non_numeric: This puts quotes around all fields that are non-numeric. Namely, when
  //   *       writing a field that does not parse as a valid float or integer, then quotes will be
  //   *       used even if they aren't strictly necessary.
  //   *
  //   * @note
  //   *   compression is not supported for this format.
  //   * @param filePath
  //   *   output file location
  //   */
  // def csv(filePath: String): Unit =
  //   writeCSV(
  //     ptr = ptr,
  //     filePath = filePath,
  //     options = jsonMapper.writeValueAsString(_options)
  //   )

  // /** Saves the content of the [[DataFrame]] in JSON format at the specified path (local and
  //   * cloud).
  //   *
  //   * A single JSON array containing each DataFrame row as an object. The length of the array is
  //   * the number of rows in the DataFrame. Use this to create valid JSON that can be deserialized
  //   * back into an array in one fell swoop.
  //   *
  //   * @note
  //   *   compression is not supported for this format.
  //   *
  //   * @param filePath
  //   *   output file location
  //   */
  // def json(filePath: String): Unit = {
  //   option("write_json_format", "json")
  //   writeJson(
  //     ptr = ptr,
  //     filePath = filePath,
  //     options = jsonMapper.writeValueAsString(_options)
  //   )
  // }

  // /** Saves the content of the [[DataFrame]] in Newline Delimited JSON (ndjson) format at the
  //   * specified path (local and cloud).
  //   *
  //   * Each DataFrame row is serialized as a JSON object on a separate line. The number of lines in
  //   * the output is the number of rows in the DataFrame.
  //   *
  //   * The [[https://pola-rs.github.io/polars/py-polars/html/reference/config.html JSON Lines]]
  //   * format makes it easy to read records in a streaming fashion, one (line) at a time. But the
  //   * output in its entirety is not valid JSON; only the individual lines are. It is recommended
  //   * to use the file extension `.jsonl` when saving as JSON Lines.
  //   *
  //   * @note
  //   *   compression is not supported for this format.
  //   * @param filePath
  //   *   output file location
  //   */
  // def json_lines(filePath: String): Unit = {
  //   option("write_json_format", "json_lines")
  //   writeJson(
  //     ptr = ptr,
  //     filePath = filePath,
  //     options = jsonMapper.writeValueAsString(_options)
  //   )
  // }
}
