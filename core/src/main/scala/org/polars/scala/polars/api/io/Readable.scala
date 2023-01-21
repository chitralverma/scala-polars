package org.polars.scala.polars.api.io

import org.polars.scala.polars.Polars
import org.polars.scala.polars.api.DataFrame

class Readable private[polars] () {

  def parquet(filePath: String, filePaths: String*)(
      nRows: Option[Long] = None,
      cache: Boolean = true,
      reChunk: Boolean = false,
      lowMemory: Boolean = false,
      rowCountColName: Option[String] = None,
      rowCountColOffset: Option[Int] = Some(0)
  ): DataFrame = Polars.scan
    .parquet(filePath, filePaths: _*)(
      nRows,
      cache,
      reChunk,
      lowMemory,
      rowCountColName,
      rowCountColOffset
    )
    .collect()

  def csv(filePath: String, filePaths: String*)(
      nRows: Option[Long] = None,
      delimiter: Char = ',',
      hasHeader: Boolean = true,
      inferSchemaRows: Long = 100,
      skipRowsAfterHeader: Int = 0,
      ignoreErrors: Boolean = false,
      parseDates: Boolean = false,
      cache: Boolean = true,
      reChunk: Boolean = false,
      lowMemory: Boolean = false,
      rowCountColName: Option[String] = None,
      rowCountColOffset: Option[Int] = Some(0)
  ): DataFrame = Polars.scan
    .csv(filePath, filePaths: _*)(
      nRows,
      delimiter,
      hasHeader,
      inferSchemaRows,
      skipRowsAfterHeader,
      ignoreErrors,
      parseDates,
      cache,
      reChunk,
      lowMemory,
      rowCountColName,
      rowCountColOffset
    )
    .collect()

  def ndJson(filePath: String, filePaths: String*)(
      nRows: Option[Long] = None,
      inferSchemaRows: Long = 100,
      cache: Boolean = true,
      reChunk: Boolean = false,
      lowMemory: Boolean = false,
      rowCountColName: Option[String] = None,
      rowCountColOffset: Option[Int] = Some(0)
  ): DataFrame = Polars.scan
    .ndJson(filePath, filePaths: _*)(
      nRows,
      inferSchemaRows,
      cache,
      reChunk,
      lowMemory,
      rowCountColName,
      rowCountColOffset
    )
    .collect()

  def ipc(filePath: String, filePaths: String*)(
      nRows: Option[Long] = None,
      cache: Boolean = true,
      reChunk: Boolean = false,
      memMap: Boolean = true,
      rowCountColName: Option[String] = None,
      rowCountColOffset: Option[Int] = Some(0)
  ): DataFrame = Polars.scan
    .ipc(filePath, filePaths: _*)(
      nRows,
      cache,
      reChunk,
      memMap,
      rowCountColName,
      rowCountColOffset
    )
    .collect()
}
