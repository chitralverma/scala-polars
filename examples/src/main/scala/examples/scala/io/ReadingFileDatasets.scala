package examples.scala.io

import org.polars.scala.polars.Polars
import org.polars.scala.polars.api.{DataFrame, LazyFrame}

import examples.scala.utils.CommonUtils

/** Polars supports various input file formats like the following,
  *   - [[Polars.csv CSV]] (delimited format like CSV, TSV, etc.)
  *   - [[org.polars.scala.polars.api.io.Scannable.parquet Apache Parquet]]
  *   - [[Polars.ipc Apache Arrow IPC]]
  *   - [[Polars.ndJson New line delimited JSON]]
  *
  * All the above formats are compatible with the lazy or eager input API and users can supply 1
  * or more file paths which will be read in parallel to return a [[LazyFrame]] or a
  * [[DataFrame]].
  *
  * Since each format may have its own additional options (example: delimiter for CSV format),
  * Polars allows a simple builder pattern which can be used to supply these options.
  *
  * While the examples below have been provided for Parquet files only, they also similarly apply
  * on the other supported file formats.
  *
  * Some additional examples may also be found in [[examples.scala.io.LazyAndEagerAPI]].
  */

object ReadingFileDatasets {

  def main(args: Array[String]): Unit = {

    /* For one Parquet file */
    val path = CommonUtils.getResource("/files/web-ds/data.parquet")
    val df = Polars.scan
      .parquet(path)
      .collect()

    println("Showing parquet file as a DataFrame to stdout.")
    df.show()

    printf("Total rows: %s%n%n", df.count())

    /* For multiple Parquet file(s) */
    val multiLdf = Polars.scan.parquet(path, path, path).collect()

    println("Showing multiple parquet files as 1 DataFrame to stdout.")
    multiLdf.show()
    printf("Total rows: %s%n%n", multiLdf.count())

    /* Providing additional options with Parquet file input */
    val pqDfWithOpts = Polars.scan
      .option("scan_parquet_low_memory", "true")
      .option("scan_parquet_n_rows", "3")
      .option("scan_parquet_cache", "false")
      .option("scan_parquet_row_index_name", "SerialNum")
      .parquet(path)
      .collect()

    println("Showing parquet file as a DataFrame to stdout.")
    pqDfWithOpts.show()

    printf("Total rows: %s%n%n", pqDfWithOpts.count())

  }

}
