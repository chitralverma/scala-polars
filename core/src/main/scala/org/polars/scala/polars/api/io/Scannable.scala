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
    *     scanned if not specified. Default: `null`
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
    *     be skipped from reading. Default: `true`
    *   - `scan_parquet_cache`: Cache the result after scanning. Default: `true`
    *   - `scan_parquet_glob`: Expand path given via globbing rules. Default: `true`
    *   - `scan_parquet_low_memory`: Reduce memory pressure at the expense of performance.
    *     Default: `false`
    *   - `scan_parquet_rechunk`: In case of reading multiple files via a glob pattern re-chunk
    *     the final DataFrame into contiguous memory chunks. Default: `false`
    *   - `scan_parquet_allow_missing_columns`: When reading a list of parquet files, if a column
    *     existing in the first file cannot be found in subsequent files, the default behavior is
    *     to raise an error. However, if this option is set to `true`, a full-NULL column is
    *     returned instead of throwing error for the files that do not contain the column.
    *     Default: `false`
    *   - `scan_parquet_include_file_paths`: If set, this option includes the path of the source
    *     file(s) as a column with provided name. Default: `null`
    *   - `scan_parquet_hive_scan_partitions`: Infer statistics and schema from hive partitions
    *     and use them to prune reads. Set as `false` to automatically enable for single directory
    *     scans. Default: `true`
    *   - `scan_parquet_hive_try_parse_dates`: Whether to try parsing hive values as date/datetime
    *     types. Default: `true`
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
    *     not specified. Default: `null`
    *   - `scan_ipc_cache`: Cache the result after scanning. Default: `true`
    *   - `scan_ipc_rechunk`: In case of reading multiple files via a glob pattern re-chunk the
    *     final DataFrame into contiguous memory chunks. Default: `false`
    *   - `scan_ipc_row_index_name`: If set, this inserts a row index column with the given name.
    *     Default: `null`
    *   - `scan_ipc_row_index_offset`: Sets the offset (>=0) to start the row index column (only
    *     used if `scan_ipc_row_index_name` is set). Default: 0
    *   - `scan_ipc_include_file_paths`: If set, this option includes the path of the source
    *     file(s) as a column with provided name. Default: `null`
    *   - `scan_ipc_hive_scan_partitions`: Infer statistics and schema from hive partitions and
    *     use them to prune reads. Set as `false` to automatically enable for single directory
    *     scans. Default: `true`
    *   - `scan_ipc_hive_try_parse_dates`: Whether to try parsing hive values as date/datetime
    *     types. Default: `true`
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

  /** Scans the contents of a dataset in CSV format from the specified path(s) (local and cloud).
    * Provided paths support globbing and expansion.
    *
    * Supported options:
    *   - `scan_csv_n_rows`: Sets the maximum rows to read from csv files. All rows are scanned if
    *     not specified. Default: `null`.
    *   - `scan_csv_encoding`: Sets the encoding of the csv file to scan. Supported values
    *     ‘lossy_utf8’, ‘utf8’.
    *     - utf8 (default): Utf8 encoding.
    *     - lossy_utf8: Utf8 encoding and unknown bytes are replaced with �.
    *   - `scan_csv_row_index_name`: If set, this inserts a row index column with the given name.
    *     Default: `null`
    *   - `scan_csv_row_index_offset`: Sets the offset (>=0) to start the row index column (only
    *     used if `scan_csv_row_index_name` is set). Default: 0
    *   - `scan_csv_cache`: Cache the result after scanning. Default: `true`
    *   - `scan_csv_glob`: Expand path given via globbing rules. Default: `true`
    *   - `scan_csv_low_memory`: Reduce memory pressure at the expense of performance. Default:
    *     `false`
    *   - `scan_csv_rechunk`: In case of reading multiple files via a glob pattern re-chunk the
    *     final DataFrame into contiguous memory chunks. Default: `false`
    *   - `scan_csv_include_file_paths`: If set, this option includes the path of the source
    *     file(s) as a column with provided name. Default: `null`
    *   - `scan_csv_raise_if_empty`: When there is no data in the source, NoDataError is raised.
    *     If this parameter is set to False, an empty LazyFrame (with no columns) is returned
    *     instead. Default: `true`
    *   - `scan_csv_ignore_errors`: Try to keep reading lines if some lines yield errors. Default:
    *     `false`
    *   - `scan_csv_has_header`: Indicate if the first row of the dataset is a header or not. If
    *     set to False, column names will be autogenerated in the following format: column_x, with
    *     x being an enumeration over every column in the dataset, starting at 1. Default: `true`
    *   - `scan_csv_missing_is_null`: Treat missing fields as null. Default: `true`
    *   - `scan_csv_truncate_ragged_lines`: Truncate lines that are longer than the schema.
    *     Default: `false`
    *   - `scan_csv_try_parse_dates`: Try to automatically parse dates. Most ISO8601-like formats
    *     can be inferred, as well as a handful of others. If this does not succeed, the column
    *     remains of data type `String`. Default: `false`
    *   - `scan_csv_decimal_comma`: Parse floats using a comma as the decimal separator instead of
    *     a period. Default: `false`
    *   - `scan_csv_chunk_size`: Sets the chunk size used by the parser. This influences
    *     performance. This can be used as a way to reduce memory usage during the parsing at the
    *     cost of performance. Default: 2^18^
    *   - `scan_csv_skip_rows`: Skip the first n rows during parsing. The header will be parsed at
    *     row n. Default: 0
    *   - `scan_csv_skip_rows_after_header`: Skip this number of rows after the header location.
    *     Default: 0
    *   - `scan_csv_skip_infer_schema_length`: Sets the number of rows to use when inferring the
    *     csv schema. Setting to `null` will do a full table scan which is very slow. Default: 100
    *   - `scan_csv_separator`: Sets the CSV file's column separator as a byte character. Default:
    *     `,`
    *   - `scan_csv_quote_char`: Set the char used as quote char. If set to `null` quoting is
    *     disabled. Default: `"`
    *   - `scan_csv_eol_char`: Set the char used as end of line. Default: `\n`
    *   - `scan_csv_null_value`: Value to interpret as null/ missing value, all values equal to
    *     this string will be null. Default: `null`
    *   - `scan_csv_comment_prefix`: A string used to indicate the start of a comment line.
    *     Comment lines are skipped during parsing. Common examples of comment prefixes are # and
    *     //. Default: `null`
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
  def csv(path: String, paths: String*): LazyFrame =
    LazyFrame.withPtr(
      scanCSV(
        paths = paths.+:(path).toArray[String],
        options = jsonMapper.writeValueAsString(_options)
      )
    )

  /** Scans the contents of a dataset in Newline Delimited JSON (ndjson) format from the specified
    * path(s) (local and cloud). Provided paths support globbing and expansion.
    *
    * Supported options:
    *   - `scan_ndjson_n_rows`: Sets the maximum rows to read from ndjson files. All rows are
    *     scanned if not specified. During multithreaded parsing the upper bound n cannot be
    *     guaranteed. Default: `null`
    *   - `scan_ndjson_row_index_name`: If set, this inserts a row index column with the given
    *     name. Default: `null`
    *   - `scan_ndjson_row_index_offset`: Sets the offset (>=0) to start the row index column
    *     (only used if `scan_ndjson_row_index_name` is set). Default: 0
    *   - `scan_ndjson_low_memory`: Reduce memory pressure at the expense of performance. Default:
    *     `false`
    *   - `scan_ndjson_rechunk`: In case of reading multiple files via a glob pattern re-chunk the
    *     final DataFrame into contiguous memory chunks. Default: `false`
    *   - `scan_ndjson_include_file_paths`: If set, this option includes the path of the source
    *     file(s) as a column with provided name. Default: `null`
    *   - `scan_ndjson_ignore_errors`: Try to keep reading lines if parsing fails because of
    *     schema mismatches. Default: `false`
    *   - `scan_ndjson_batch_size`: Sets the number of rows to read in each batch. This influences
    *     performance. This can be used as a way to reduce memory usage during the parsing at the
    *     cost of performance. Default: `null`
    *   - `scan_ndjson_infer_schema_length`: Sets the number of rows to use when inferring the
    *     json schema. Setting to `null` will do a full table scan which is very slow. Default:
    *     100
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
  def jsonLines(path: String, paths: String*): LazyFrame =
    LazyFrame.withPtr(
      scanJsonLines(
        paths = paths.+:(path).toArray[String],
        options = jsonMapper.writeValueAsString(_options)
      )
    )
}
