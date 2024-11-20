package org.polars.scala.polars.internal.jni

private[polars] object row extends Natively {

  @native def createIterator(dfPtr: Long, nRows: Long): Long

  @native def advanceIterator(ptr: Long): Array[Object]

  @native def schemaString(ptr: Long): String

}
