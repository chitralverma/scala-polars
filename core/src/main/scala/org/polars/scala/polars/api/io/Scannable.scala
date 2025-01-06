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
    * cloud).
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

}
