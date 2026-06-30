package com.github.chitralverma.polars.internal.jni

private[polars] object series extends Natively {

  @native def show(ptr: Long): Unit

  @native def newStrSeries(name: String, data: Array[String]): Long

  @native def newIntSeries(name: String, data: Array[Int]): Long

  @native def newFloatSeries(name: String, data: Array[Float]): Long

  @native def newDoubleSeries(name: String, data: Array[Double]): Long

  @native def newLongSeries(name: String, data: Array[Long]): Long

  @native def newBooleanSeries(name: String, data: Array[Boolean]): Long

  @native def newDateSeries(name: String, data: Array[String]): Long

  @native def newDatetimeSeries(name: String, data: Array[String]): Long

  @native def newTimeSeries(name: String, data: Array[String]): Long

  @native def newListSeries(name: String, ptrs: Array[Long]): Long

  @native def newStructSeries(name: String, ptrs: Array[Long]): Long

  @native def free(ptr: Long): Unit

}
