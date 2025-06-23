package com.github.chitralverma.polars

import com.github.chitralverma.polars.api.io.Scannable
import com.github.chitralverma.polars.api.{DataFrame, LazyFrame}
import com.github.chitralverma.polars.config.Config
import com.github.chitralverma.polars.internal.jni.{common, data_frame, lazy_frame}

object Polars {

  def config: Config = Config.getConfig

  def version(): String = common.version()

  /** Returns a [[com.github.chitralverma.polars.api.io.Scannable Scannable]] that
    * can be used to lazily scan datasets of various formats
    * ([[com.github.chitralverma.polars.api.io.Scannable.parquet parquet]],
    * [[com.github.chitralverma.polars.api.io.Scannable.ipc ipc]],
    * [[com.github.chitralverma.polars.api.io.Scannable.csv csv]] and
    * [[com.github.chitralverma.polars.api.io.Scannable.jsonLines jsonLines]]) from
    * local filesystems and cloud object stores (aws, gcp and azure) as a
    * [[com.github.chitralverma.polars.api.LazyFrame LazyFrame]].
    * @return
    *   [[com.github.chitralverma.polars.api.io.Scannable Scannable]]
    */
  def scan: Scannable = new Scannable()

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
