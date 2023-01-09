package org.polars.scala.polars

import org.polars.scala.polars.api.LazyFrame
import org.polars.scala.polars.api.io.{Readable, Scannable}
import org.polars.scala.polars.config.Config
import org.polars.scala.polars.internal.jni.Natively
import org.polars.scala.polars.internal.jni.common._

object Polars extends Natively {

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

}
