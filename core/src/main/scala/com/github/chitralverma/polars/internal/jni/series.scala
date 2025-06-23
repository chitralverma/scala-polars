package com.github.chitralverma.polars.internal.jni

private[polars] object series extends Natively {

  @native def show(ptr: Long): Unit

  @native def new_str_series(name: String, data: Array[String]): Long

  @native def new_int_series(name: String, data: Array[Int]): Long

  @native def new_float_series(name: String, data: Array[Float]): Long

  @native def new_double_series(name: String, data: Array[Double]): Long

  @native def new_long_series(name: String, data: Array[Long]): Long

  @native def new_boolean_series(name: String, data: Array[Boolean]): Long

  @native def new_date_series(name: String, data: Array[String]): Long

  @native def new_datetime_series(name: String, data: Array[String]): Long

  @native def new_time_series(name: String, data: Array[String]): Long

  @native def new_list_series(name: String, ptrs: Array[Long]): Long

  @native def new_struct_series(name: String, ptrs: Array[Long]): Long

}
