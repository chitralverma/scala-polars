package org.polars.scala.polars.api

import org.polars.scala.polars.internal.jni.Natively

class LazyFrame extends Natively {

  @native def collect(): Unit

  @native def fetch(nRows: Long): Unit

}
