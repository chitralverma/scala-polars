package org.polars.scala.polars.internal.jni.io

import org.polars.scala.polars.internal.jni.Natively

private[polars] object ndjson extends Natively {

  @native def _scanNdJson(
      filePath: String,
      nRows: Long,
      inferSchemaRows: Long,
      rowCountColName: String,
      rowCountColOffset: Int
  ): Long

}
