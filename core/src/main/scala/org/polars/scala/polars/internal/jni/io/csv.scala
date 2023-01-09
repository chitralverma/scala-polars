package org.polars.scala.polars.internal.jni.io

import org.polars.scala.polars.internal.jni.Natively

private[polars] object csv extends Natively {

  @native def _scanCSV(
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

}
