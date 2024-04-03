package examples.scala.io

import org.polars.scala.polars.Polars
import org.polars.scala.polars.api.DataFrame

import examples.scala.utils.CommonUtils

/** Polars supports exporting the contents of a [[DataFrame]] to JSON.
  *
  * It has 2 formats:
  *   - a row-oriented format, which represents the frame as an array of objects whose keys are
  *     the column names and whose values are the row’s corresponding values.
  *   - a column-oriented format, which represents the frame as an array of objects containing a
  *     column name, type, and the array of column values
  *
  * The column-oriented format may be pretty-printed. The row-oriented format is less efficient,
  * but may be more convenient for downstream applications.
  */
object Json {

  def main(args: Array[String]) = {

    val path = CommonUtils.getResource("/files/web-ds/data.csv")
    val df: DataFrame = Polars.csv.scan(path).collect

    println("Showing CSV file as a DataFrame to stdout.")
    df.show()

    println("Showing column-oriented CSV file as a DataFrame to stdout.")
    val colOriented = df.write().toJsonString(pretty = false, rowOriented = false)
    println(colOriented)

    println("Showing pretty column-oriented CSV file as a DataFrame to stdout.")
    val prettyOriented = df.write().toJsonString(pretty = true, rowOriented = false)
    println(prettyOriented)

    println("Showing row column-oriented CSV file as a DataFrame to stdout.")
    val rowOriented = df.write().toJsonString(pretty = false, rowOriented = true)
    println(rowOriented)

  }

}
