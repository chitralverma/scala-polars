package com.github.chitralverma.polars.testing

import com.github.chitralverma.polars.api.{DataFrame, Series}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Shared base for Scala test suites — a reusable test base, not a per-suite fixture. Suites extend
  * it for frame-construction and assertion helpers, and should name the upstream pytest they
  * replicate (e.g. `py-polars/tests/unit/operations/test_filter.py`) at the top of the file. The
  * Java mirror is `AbstractPolarsJavaTest`.
  */
abstract class PolarsTestBase extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  /** Collect a [[DataFrame]] eagerly into row maps; values are plain JVM objects (Integer, ...). */
  protected def rowsOf(df: DataFrame): Seq[Map[String, AnyRef]] =
    df.rows().map(_.toMap).toList

  protected def columnOf(df: DataFrame, name: String): Seq[AnyRef] =
    rowsOf(df).map(_(name))

  protected def assertRowCount(df: DataFrame, expected: Long): Unit =
    df.count() shouldBe expected

  protected def assertColumns(df: DataFrame, expected: String*): Unit =
    df.schema.getFieldNames.toSeq shouldBe expected.toSeq

  protected def assertColumnValues(df: DataFrame, name: String, expected: Any*): Unit =
    columnOf(df, name) shouldBe expected.map(_.asInstanceOf[AnyRef]).toSeq

  protected def intFrame(name: String, values: Int*): DataFrame =
    DataFrame.fromSeries(Series.ofInt(name, values.toArray))

  protected def longFrame(name: String, values: Long*): DataFrame =
    DataFrame.fromSeries(Series.ofLong(name, values.toArray))

  protected def stringFrame(name: String, values: String*): DataFrame =
    DataFrame.fromSeries(Series.ofString(name, values.toArray))

  protected def doubleFrame(name: String, values: Double*): DataFrame =
    DataFrame.fromSeries(Series.ofDouble(name, values.toArray))

  protected def booleanFrame(name: String, values: Boolean*): DataFrame =
    DataFrame.fromSeries(Series.ofBoolean(name, values.toArray))
}
