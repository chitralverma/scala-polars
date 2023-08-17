package examples.scala.expressions

import java.sql.Timestamp
import java.time.Instant

import scala.util.Random

import org.polars.scala.polars.Polars
import org.polars.scala.polars.functions._

import examples.scala.utils.CommonUtils

object ApplyingSimpleExpressions {
  def main(args: Array[String]): Unit = {

    /* Read a dataset as a DataFrame lazily or eagerly */
    val path = CommonUtils.getResource("/files/web-ds/data.json")
    val input = Polars.ndJson.scan(path)

    /* Apply multiple operations on the LazyFrame or DataFrame */
    var ldf = input.cache
      .select("id", "name")
      .withColumn("lower_than_four", col("id") <= 4)
      .filter(col("lower_than_four"))
      .withColumn("long_value", lit(Random.nextLong()))
      .withColumn("current_ts", lit(Timestamp.from(Instant.now())))
      .sort(asc("name"), nullLast = true, maintainOrder = false)
      .limit(2) // .head(2)
      .tail(1)
      .drop("current_ts", "long_value")
      .rename("lower_than_four", "less_than_four")
      .drop_nulls()

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
