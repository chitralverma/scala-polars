package org.polars.scala.polars

import org.polars.scala.polars.api.io.{Readable, Scannable}
import org.polars.scala.polars.api.{DataFrame, LazyFrame}
import org.polars.scala.polars.config.Config
import org.polars.scala.polars.internal.jni.common._

object Polars {

  def config: Config = Config.getConfig

  def version(): String = _version()

  def scan: Scannable = new Scannable

  def read: Readable = new Readable

  def concat(
      lazyFrames: LazyFrame*
  )(reChunk: Boolean = false, parallel: Boolean = true): LazyFrame = {
    val ptr =
      _concatLazyFrames(lazyFrames.map(_.ptr).toArray, reChunk = reChunk, parallel = parallel)
    LazyFrame.withPtr(ptr)
  }

  def concat(dataFrames: DataFrame*): DataFrame = {
    val ptr = _concatDataFrames(dataFrames.map(_.ptr).toArray)
    DataFrame.withPtr(ptr)
  }

}

private[polars] object LibraryStates extends Enumeration {
  type LibraryState = Value

  val NOT_LOADED, LOADING, LOADED = Value
}
