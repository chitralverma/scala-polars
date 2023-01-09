package org.polars.scala.polars.api

import org.polars.scala.polars.internal.jni.{Natively, lazy_frame}

class LazyFrame private (private[polars] val ptr: Long) extends Natively {

  def collect(): DataFrame = {
    val dfPtr = lazy_frame.collect(ptr)
    DataFrame.withPtr(dfPtr)
  }

}

object LazyFrame {

  private[polars] def withPtr(ptr: Long) = new LazyFrame(ptr)
}
