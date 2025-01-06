package org.polars.scala.polars.internal.jni.io

import org.polars.scala.polars.internal.jni.Natively

private[polars] object scan extends Natively {

  @native def scanParquet(paths: Array[String], options: String): Long

  @native def scanCSV(
      filePath: String,
      nRows: Long,
      delimiter: Char,
      hasHeader: Boolean,
      inferSchemaRows: Long,
      skipRowsAfterHeader: Int,
      ignoreErrors: Boolean,
      parseDates: Boolean,
      cache: Boolean,
      reChunk: Boolean,
      lowMemory: Boolean,
      rowCountColName: String,
      rowCountColOffset: Int
  ): Long

  @native def scanNdJson(
      filePath: String,
      nRows: Long,
      inferSchemaRows: Long,
      cache: Boolean,
      reChunk: Boolean,
      lowMemory: Boolean,
      rowCountColName: String,
      rowCountColOffset: Int
  ): Long

  @native def scanIPC(
      filePath: String,
      nRows: Long,
      cache: Boolean,
      reChunk: Boolean,
      memMap: Boolean,
      rowCountColName: String,
      rowCountColOffset: Int
  ): Long

}
