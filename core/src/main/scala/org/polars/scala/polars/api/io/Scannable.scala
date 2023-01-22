package org.polars.scala.polars.api.io

import scala.collection.parallel.{immutable => ParallelCollections}

import org.polars.scala.polars.Polars
import org.polars.scala.polars.api.LazyFrame
import org.polars.scala.polars.internal.jni.io.scan._

class Scannable private[polars] () {

  def concat(f: => Seq[LazyFrame]): LazyFrame = {
    val lazyFrames = f

    lazyFrames match {
      case ldf :: Nil => ldf
      case ldf :: others => Polars.concat(ldf, others: _*)()
    }
  }

  def parquet(filePath: String, filePaths: String*)(
      nRows: Option[Long] = None,
      cache: Boolean = true,
      reChunk: Boolean = false,
      lowMemory: Boolean = false,
      rowCountColName: Option[String] = None,
      rowCountColOffset: Option[Int] = Some(0)
  ): LazyFrame = concat {
    ParallelCollections.ParSeq
      .concat(filePaths.+:(filePath))
      .map { path =>
        val ptr = scanParquet(
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
      .toList
  }

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
  ): LazyFrame = concat {
    ParallelCollections.ParSeq
      .concat(filePaths.+:(filePath))
      .map { path =>
        val ptr = scanCSV(
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
      .toList
  }

  def ndJson(filePath: String, filePaths: String*)(
      nRows: Option[Long] = None,
      inferSchemaRows: Long = 100,
      cache: Boolean = true,
      reChunk: Boolean = false,
      lowMemory: Boolean = false,
      rowCountColName: Option[String] = None,
      rowCountColOffset: Option[Int] = Some(0)
  ): LazyFrame = concat {
    ParallelCollections.ParSeq
      .concat(filePaths.+:(filePath))
      .map { path =>
        val ptr = scanNdJson(
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
      .toList
  }

  def ipc(filePath: String, filePaths: String*)(
      nRows: Option[Long] = None,
      cache: Boolean = true,
      reChunk: Boolean = false,
      memMap: Boolean = true,
      rowCountColName: Option[String] = None,
      rowCountColOffset: Option[Int] = Some(0)
  ): LazyFrame = concat {
    ParallelCollections.ParSeq
      .concat(filePaths.+:(filePath))
      .map { path =>
        val ptr = scanIPC(
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
      .toList
  }

}
