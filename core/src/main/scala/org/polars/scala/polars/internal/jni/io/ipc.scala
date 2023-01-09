package org.polars.scala.polars.internal.jni.io

import org.polars.scala.polars.internal.jni.Natively

private[polars] object ipc extends Natively {

  @native def _scanIPC(
      filePath: String,
      nRows: Long,
      cache: Boolean,
      reChunk: Boolean,
      memMap: Boolean,
      rowCountColName: String,
      rowCountColOffset: Int
  ): Long

}
