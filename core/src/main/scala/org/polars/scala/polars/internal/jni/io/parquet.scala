package org.polars.scala.polars.internal.jni.io

import org.polars.scala.polars.api.LazyFrame
import org.polars.scala.polars.internal.jni.Natively

private[polars] object parquet extends Natively {

  @native def _scanParquet(
      filePath: String,
      nRows: Long,
      rowCountColName: String,
      rowCountColOffset: Int
  ): Long

  def scanParquet(
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

}
