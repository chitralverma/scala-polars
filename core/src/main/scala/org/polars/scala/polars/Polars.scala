package org.polars.scala.polars

import org.polars.scala.polars.api.io.Scannable
import org.polars.scala.polars.api.io.builders._
import org.polars.scala.polars.api.{DataFrame, LazyFrame}
import org.polars.scala.polars.config.Config
import org.polars.scala.polars.internal.jni.{common, data_frame, lazy_frame}

object Polars {

  def config: Config = Config.getConfig

  def version(): String = common.version()

  def scan: Scannable = new Scannable()

  def csv: CSVInputBuilder = new CSVInputBuilder()

  def ipc: IPCInputBuilder = new IPCInputBuilder()

  def ndJson: NdJsonInputBuilder = new NdJsonInputBuilder()

  def concat(lazyFrame: LazyFrame, lazyFrames: Array[LazyFrame]): LazyFrame =
    concat(lazyFrame, lazyFrames, reChunk = false, parallel = true)

  def concat(
      lazyFrame: LazyFrame,
      lazyFrames: Array[LazyFrame],
      reChunk: Boolean = false,
      parallel: Boolean = true
  ): LazyFrame =
    if (lazyFrames.isEmpty) lazyFrame
    else {
      val ptr =
        lazy_frame.concatLazyFrames(
          lazyFrames.+:(lazyFrame).map(_.ptr),
          reChunk = reChunk,
          parallel = parallel
        )

      LazyFrame.withPtr(ptr)
    }

  def concat(dataFrame: DataFrame, dataFrames: Array[DataFrame]): DataFrame =
    if (dataFrames.isEmpty) dataFrame
    else {
      val ptr = data_frame.concatDataFrames(dataFrames.+:(dataFrame).map(_.ptr))

      DataFrame.withPtr(ptr)
    }

}

private[polars] object LibraryStates extends Enumeration {
  type LibraryState = Value

  val NOT_LOADED, LOADING, LOADED = Value
}
