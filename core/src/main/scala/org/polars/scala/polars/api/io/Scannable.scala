package org.polars.scala.polars.api.io

import org.polars.scala.polars.Polars
import org.polars.scala.polars.api.LazyFrame
import org.polars.scala.polars.internal.jni.io.csv._scanCSV
import org.polars.scala.polars.internal.jni.io.ipc._scanIPC
import org.polars.scala.polars.internal.jni.io.ndjson._scanNdJson
import org.polars.scala.polars.internal.jni.io.parquet._scanParquet

class Scannable private[polars] () {

  def parquet(filePaths: String*)(
      nRows: Option[Long] = None,
      cache: Boolean = true,
      reChunk: Boolean = false,
      lowMemory: Boolean = false,
      rowCountColName: Option[String] = None,
      rowCountColOffset: Option[Int] = Some(0)
  ): LazyFrame = {
    val lazyFrames = filePaths.map { path =>
      val ptr = _scanParquet(
        path,
        nRows.getOrElse(-1),
        cache,
        reChunk,
        lowMemory,
        rowCountColName.orNull,
        rowCountColOffset.getOrElse(0)
      )

      LazyFrame.withPtr(ptr)
    }

    Polars.concat(lazyFrames: _*)()
  }

  def csv(filePaths: String*)(
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
  ): LazyFrame = {
    val lazyFrames = filePaths.map { path =>
      val ptr = _scanCSV(
        path,
        nRows.getOrElse(-1),
        delimiter,
        hasHeader,
        inferSchemaRows,
        skipRowsAfterHeader,
        ignoreErrors,
        parseDates,
        cache,
        reChunk,
        lowMemory,
        rowCountColName.orNull,
        rowCountColOffset.getOrElse(0)
      )

      LazyFrame.withPtr(ptr)
    }

    Polars.concat(lazyFrames: _*)()
  }

  def ndJson(filePaths: String*)(
      nRows: Option[Long] = None,
      inferSchemaRows: Long = 100,
      cache: Boolean = true,
      reChunk: Boolean = false,
      lowMemory: Boolean = false,
      rowCountColName: Option[String] = None,
      rowCountColOffset: Option[Int] = Some(0)
  ): LazyFrame = {
    val lazyFrames = filePaths.map { path =>
      val ptr = _scanNdJson(
        path,
        nRows.getOrElse(-1),
        inferSchemaRows,
        cache,
        reChunk,
        lowMemory,
        rowCountColName.orNull,
        rowCountColOffset.getOrElse(0)
      )
      LazyFrame.withPtr(ptr)
    }

    Polars.concat(lazyFrames: _*)()
  }

  def ipc(filePaths: String*)(
      nRows: Option[Long] = None,
      cache: Boolean = true,
      reChunk: Boolean = false,
      memMap: Boolean = true,
      rowCountColName: Option[String] = None,
      rowCountColOffset: Option[Int] = Some(0)
  ): LazyFrame = {
    val lazyFrames = filePaths.map { path =>
      val ptr = _scanIPC(
        path,
        nRows.getOrElse(-1),
        cache,
        reChunk,
        memMap,
        rowCountColName.orNull,
        rowCountColOffset.getOrElse(0)
      )

      LazyFrame.withPtr(ptr)
    }

    Polars.concat(lazyFrames: _*)()
  }

}
