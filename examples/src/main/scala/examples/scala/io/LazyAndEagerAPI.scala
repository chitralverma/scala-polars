package examples.scala.io

import org.polars.scala.polars.Polars
import org.polars.scala.polars.api.{DataFrame, LazyFrame}

import examples.scala.utils.CommonUtils

/** Polars provides 2 API for reading datasets lazily (`scan`) or eagerly (`read`).
  *
  * These APIs serve different purposes and result in either a [[LazyFrame]] or a [[DataFrame]]. A
  * LazyFrame can be materialized to a DataFrame and vice-versa if required.
  * ==Lazy API==
  * With the lazy API Polars doesn't run each query line-by-line but instead processes the full
  * query end-to-end. To get the most out of Polars it is important that you use the lazy API
  * because:
  *
  *   - the lazy API allows Polars to apply automatic query optimization with the query optimizer.
  *   - the lazy API allows you to work with larger than memory datasets using streaming.
  *   - the lazy API can catch schema errors before processing the data.
  *
  * More info can be found
  * [[https://pola-rs.github.io/polars-book/user-guide/lazy-api/intro.html here]].
  * ==Eager API==
  * With eager API the queries are executed line-by-line in contrast to the lazy API.
  */

object LazyAndEagerAPI {

  def main(args: Array[String]): Unit = {
    /* Lazily read data from file based datasets */
    val path = CommonUtils.getResource("/files/web-ds/data.csv")
    val ldf = Polars.scan.csv(path)

    /* Materialize LazyFrame to DataFrame */
    var df: DataFrame = ldf.collect()

    println("Showing CSV file as a DataFrame to stdout.")
    df.show()

    printf("Total rows: %s%n%n", df.count())
    printf("Total columns: %s%n%n", df.schema.getFields.length)

    /* Lazily read only first 3 rows */
    df = Polars.scan.option("scan_csv_n_rows", "3").csv(path).collect()
    printf("Total rows: %s%n%n", df.count())

    println("Rows:")
    df.rows().foreach(println)
    println("\n")

    /* Convert DataFrame back to LazyFrame */
    val backToLdf: LazyFrame = df.toLazy
    printf("Show schema: %s%n%n", backToLdf.schema)

    /* Eagerly read data from file based datasets */
    df = Polars.scan.csv(path).collect

    println("Showing CSV file as a DataFrame to stdout")
    df.show()

  }

}
