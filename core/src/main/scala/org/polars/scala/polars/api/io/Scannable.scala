package org.polars.scala.polars.api.io

import org.polars.scala.polars.api.LazyFrame
import org.polars.scala.polars.internal.jni.io.csv._scanCSV
import org.polars.scala.polars.internal.jni.io.ndjson._scanNdJson
import org.polars.scala.polars.internal.jni.io.parquet._scanParquet

class Scannable private[polars] () {

  def parquet(
      filePath: String,
      nRows: Option[Long] = None,
      rowCountColName: Option[String] = None,
      rowCountColOffset: Option[Int] = Some(0)
  ): LazyFrame = {
    val ptr = _scanParquet(
      filePath,
      nRows.getOrElse(-1),
      rowCountColName.orNull,
      rowCountColOffset.getOrElse(0)
    )

    LazyFrame.withPtr(ptr)
  }

  def csv(
      filePath: String,
      nRows: Option[Long] = None,
      delimiter: Char = ',',
      hasHeader: Boolean = true,
      inferSchemaRows: Long = 100,
      skipRowsAfterHeader: Int = 0,
      ignoreErrors: Boolean = false,
      parseDates: Boolean = false,
      rowCountColName: Option[String] = None,
      rowCountColOffset: Option[Int] = Some(0)
  ): LazyFrame = {
    val ptr = _scanCSV(
      filePath,
      nRows.getOrElse(-1),
      delimiter,
      hasHeader,
      inferSchemaRows,
      skipRowsAfterHeader,
      ignoreErrors,
      parseDates,
      rowCountColName.orNull,
      rowCountColOffset.getOrElse(0)
    )

    LazyFrame.withPtr(ptr)
  }

  def ndJson(
      filePath: String,
      nRows: Option[Long] = None,
      inferSchemaRows: Long = 100,
      rowCountColName: Option[String] = None,
      rowCountColOffset: Option[Int] = Some(0)
  ): LazyFrame = {
    val ptr = _scanNdJson(
      filePath,
      nRows.getOrElse(-1),
      inferSchemaRows,
      rowCountColName.orNull,
      rowCountColOffset.getOrElse(0)
    )

    LazyFrame.withPtr(ptr)
  }
}
