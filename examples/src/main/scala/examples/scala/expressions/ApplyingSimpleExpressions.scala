package examples.scala.expressions

import scala.util.Random

import com.github.chitralverma.polars.Polars
import com.github.chitralverma.polars.functions._
import examples.scala.utils.CommonUtils

object ApplyingSimpleExpressions {
  def main(args: Array[String]): Unit = {

    /* Read a dataset as a DataFrame lazily or eagerly */
    val path = CommonUtils.getResource("/files/web-ds/data.json")
    val input = Polars.scan.jsonLines(path)

    /* Apply multiple operations on the LazyFrame or DataFrame */
    var ldf = input.cache
      .select("id", "name")
      .withColumn("lower_than_four", col("id") <= 4)
      .filter(col("lower_than_four"))
      .withColumn("long_value", lit(Random.nextLong()))
      .withColumn("date", lit(java.time.LocalDate.now()))
      .withColumn("time", lit(java.time.LocalTime.now()))
      .withColumn("current_ts", lit(java.time.ZonedDateTime.now()))
      .sort(asc("name"), nullLast = true, maintainOrder = false)
      .setSorted("name", descending = false)
      .topK(2, "id", descending = true, nullLast = true, maintainOrder = false)
      .limit(2) // .head(2)
      .tail(2)
      .drop("long_value")
      .rename("lower_than_four", "less_than_four")
      .dropNulls()

    ldf = Polars.concat(ldf, Array(ldf, ldf))
    ldf = ldf.unique()

    println("Showing LazyFrame plan to stdout.")
    ldf.explain()

    val df = ldf.collect()

    println("Showing resultant DataFrame to stdout.")
    df.show()

    printf("Total rows: %s%n%n", df.count())

  }

}
