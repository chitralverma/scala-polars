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
    val ldf = Polars.ndJson.scan(path)

    /* Apply multiple operations on the LazyFrame or DataFrame */
    val df = ldf
      .select("id", "name")
      .withColumn("lower_than_four", col("id") <= 4)
      .filter(col("lower_than_four"))
      .withColumn("long_value", lit(Random.nextLong()))
      .withColumn("current_ts", lit(Timestamp.from(Instant.now())))
      .sort(asc("name"), nullLast = false, maintainOrder = false)
      .collect()

    println("Showing resultant DataFrame to stdout.")
    df.show()

    printf("Total rows: %s%n%n", df.count())

  }

}
