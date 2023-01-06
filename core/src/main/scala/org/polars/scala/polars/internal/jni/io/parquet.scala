package org.polars.scala.polars.internal.jni.io

import org.polars.scala.polars.internal.jni.Natively

private[polars] object parquet extends Natively {

  @native def _scanParquet(
      filePath: String,
      nRows: Long,
      rowCountColName: String,
      rowCountColOffset: Int
  ): Long

}
