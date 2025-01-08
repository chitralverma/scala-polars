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

  /** Scans a dataset in Parquet format from the specified path(s) (local or cloud). Supports
    * globbing and path expansion.
    *
    * Supported options:
    *   - `scan_parquet_n_rows`: Maximum number of rows to read. Default: `null`.
    *   - `scan_parquet_parallel`: Strategy for parallelism. Supported values:
    *     - `auto` (default): Automatically determines the unit of parallelization.
    *     - `columns`: Parallelizes over columns.
    *     - `row_groups`: Parallelizes over row groups.
    *     - `prefiltered`: Evaluates pushed-down predicates in parallel and filters rows.
    *     - `none`: Disables parallelism.
    *   - `scan_parquet_row_index_name`: Adds a row index column with the specified name. Default:
    *     `null`.
    *   - `scan_parquet_row_index_offset`: Offset (≥0) for row index column (used only if
    *     `scan_parquet_row_index_name` is set). Default: `0`.
    *   - `scan_parquet_use_statistics`: Uses parquet statistics to skip unnecessary pages.
    *     Default: `true`.
    *   - `scan_parquet_cache`: Caches the scan result. Default: `true`.
    *   - `scan_parquet_glob`: Expands paths using globbing rules. Default: `true`.
    *   - `scan_parquet_low_memory`: Reduces memory usage at the cost of performance. Default:
    *     `false`.
    *   - `scan_parquet_rechunk`: Re-chunks the final DataFrame for memory contiguity when reading
    *     multiple files. Default: `false`.
    *   - `scan_parquet_allow_missing_columns`: Returns NULL columns instead of errors for missing
    *     columns across files. Default: `false`.
    *   - `scan_parquet_include_file_paths`: Includes source file paths as a column with the
    *     specified name. Default: `null`.
    *   - `scan_parquet_hive_scan_partitions`: Infers schema from Hive partitions and use them to
    *     prune reads. Default: `true`.
    *   - `scan_parquet_hive_try_parse_dates`: Attempts to parse Hive values as date/datetime.
    *     Default: `true`.
    *
    * @param path
    *   Main input file location.
    * @param paths
    *   Additional input file locations.
    *
    * @note
    *   All provided paths must belong to the same object store.
    */
  @varargs
  def parquet(path: String, paths: String*): LazyFrame =
    LazyFrame.withPtr(
      scanParquet(
        paths = paths.+:(path).toArray[String],
        options = jsonMapper.writeValueAsString(_options)
      )
    )

  /** Scans a dataset in IPC format from the specified path(s) (local or cloud). Supports globbing
    * and path expansion.
    *
    * Supported options:
    *   - `scan_ipc_n_rows`: Maximum number of rows to read. Default: `null`.
    *   - `scan_ipc_cache`: Caches the scan result. Default: `true`.
    *   - `scan_ipc_rechunk`: Re-chunks the final DataFrame for memory contiguity when reading
    *     multiple files. Default: `false`.
    *   - `scan_ipc_row_index_name`: Adds a row index column with the specified name. Default:
    *     `null`.
    *   - `scan_ipc_row_index_offset`: Offset (≥0) for row index column (used only if
    *     `scan_ipc_row_index_name` is set). Default: `0`.
    *   - `scan_ipc_include_file_paths`: Includes source file paths as a column with the specified
    *     name. Default: `null`.
    *   - `scan_ipc_hive_scan_partitions`: Infers schema from Hive partitions. Default: `true`.
    *   - `scan_ipc_hive_try_parse_dates`: Attempts to parse Hive values as date/datetime.
    *     Default: `true`.
    *
    * @param path
    *   Main input file location.
    * @param paths
    *   Additional input file locations.
    *
    * @note
    *   All provided paths must belong to the same object store.
    */
  @varargs
  def ipc(path: String, paths: String*): LazyFrame =
    LazyFrame.withPtr(
      scanIPC(
        paths = paths.+:(path).toArray[String],
        options = jsonMapper.writeValueAsString(_options)
      )
    )

  /** Scans a dataset in CSV format from the specified path(s) (local or cloud). Supports globbing
    * and path expansion.
    *
    * Supported options:
    *   - `scan_csv_n_rows`: Maximum number of rows to read. Default: `null`.
    *   - `scan_csv_encoding`: File encoding. Supported values:
    *     - `utf8` (default): UTF-8 encoding.
    *     - `lossy_utf8`: UTF-8 with invalid bytes replaced by `�`.
    *   - `scan_csv_row_index_name`: Adds a row index column with the specified name. Default:
    *     `null`.
    *   - `scan_csv_row_index_offset`: Offset (≥0) for row index column (used only if
    *     `scan_csv_row_index_name` is set). Default: `0`.
    *   - `scan_csv_cache`: Caches the scan result. Default: `true`.
    *   - `scan_csv_glob`: Expands paths using globbing rules. Default: `true`.
    *   - `scan_csv_low_memory`: Reduces memory usage at the cost of performance. Default:
    *     `false`.
    *   - `scan_csv_rechunk`: Re-chunks the final DataFrame for memory contiguity when reading
    *     multiple files. Default: `false`.
    *   - `scan_csv_include_file_paths`: Includes source file paths as a column with the specified
    *     name. Default: `null`.
    *   - `scan_csv_raise_if_empty`: Returns an empty LazyFrame instead of raising errors for
    *     empty datasets. Default: `true`.
    *   - `scan_csv_ignore_errors`: Continues parsing despite errors in some lines. Default:
    *     `false`.
    *   - `scan_csv_has_header`: Treats the first row as a header. If false, generates column
    *     names as `column_x`. Default: `true`.
    *   - `scan_csv_missing_is_null`: Treats missing fields as null values. Default: `true`.
    *   - `scan_csv_truncate_ragged_lines`: Truncates lines exceeding the schema length. Default:
    *     `false`.
    *   - `scan_csv_try_parse_dates`: Attempts to parse dates automatically. Default: `false`.
    *   - `scan_csv_decimal_comma`: Uses commas as decimal separators. Default: `false`.
    *   - `scan_csv_chunk_size`: Parser chunk size for memory optimization. Default: `2^18`.
    *   - `scan_csv_skip_rows`: Skips the first `n` rows. Default: `0`.
    *   - `scan_csv_skip_rows_after_header`: Number of rows to skip after the header. Default:
    *     `0`.
    *   - `scan_csv_skip_infer_schema_length`: Number of rows to use for schema inference.
    *     Default: `100`.
    *   - `scan_csv_separator`: Column separator character. Default: `,`.
    *   - `scan_csv_quote_char`: Quote character for values. Default: `"`.
    *   - `scan_csv_eol_char`: End-of-line character. Default: `\n`.
    *   - `scan_csv_null_value`: Value to interpret as null. Default: `null`.
    *   - `scan_csv_comment_prefix`: Prefix for comment lines. Default: `null`.
    *
    * @param path
    *   Main input file location.
    * @param paths
    *   Additional input file locations.
    *
    * @note
    *   All provided paths must belong to the same object store.
    */
  @varargs
  def csv(path: String, paths: String*): LazyFrame =
    LazyFrame.withPtr(
      scanCSV(
        paths = paths.+:(path).toArray[String],
        options = jsonMapper.writeValueAsString(_options)
      )
    )

  /** Scans the contents of a dataset in Newline Delimited JSON (NDJSON) format from the specified
    * path(s) (local and cloud). Provided paths support globbing and expansion.
    *
    * Supported options:
    *   - `scan_ndjson_n_rows`: Maximum number of rows to read. Default: `null`.
    *   - `scan_ndjson_row_index_name`: Adds a row index column with the specified name. Default:
    *     `null`.
    *   - `scan_ndjson_row_index_offset`: Offset (≥0) for row index column (used only if
    *     `scan_ndjson_row_index_name` is set). Default: `0`.
    *   - `scan_ndjson_low_memory`: Reduces memory usage at the cost of performance. Default:
    *     `false`.
    *   - `scan_ndjson_rechunk`: Re-chunks the final DataFrame for memory contiguity when reading
    *     multiple files. Default: `false`.
    *   - `scan_ndjson_include_file_paths`: Includes source file paths as a column with the
    *     specified name. Default: `null`.
    *   - `scan_ndjson_ignore_errors`: Continues parsing despite errors in some lines. Default:
    *     `false`.
    *   - `scan_ndjson_batch_size`: Number of rows to read in each batch to influence performance.
    *     Reduce this for memory efficiency at the cost of performance. Default: `null`
    *   - `scan_ndjson_infer_schema_length`: Number of rows to use for schema inference. Default:
    *     `100`.
    *
    * @param path
    *   Main input file location.
    * @param paths
    *   Additional input file locations.
    * @note
    *   All provided paths must belong to the same object store.
    */
  @varargs
  def jsonLines(path: String, paths: String*): LazyFrame =
    LazyFrame.withPtr(
      scanJsonLines(
        paths = paths.+:(path).toArray[String],
        options = jsonMapper.writeValueAsString(_options)
      )
    )
}
