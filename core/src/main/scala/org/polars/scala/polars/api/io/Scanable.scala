package org.polars.scala.polars.api.io

import org.polars.scala.polars.api.LazyFrame
import org.polars.scala.polars.internal.jni.io.Scan

class Scanable {

  def parquet(
      filePath: String,
      nRows: Option[Long] = None,
      rowCountColName: Option[String] = None,
      rowCountColOffset: Option[Int] = Some(0)
  ): LazyFrame = {
    Scan._scanParquet(filePath)
    new LazyFrame
  }
}
