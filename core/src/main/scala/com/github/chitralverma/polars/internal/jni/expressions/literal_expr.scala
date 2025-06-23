package com.github.chitralverma.polars.internal.jni.expressions

import com.github.chitralverma.polars.internal.jni.Natively

private[polars] object literal_expr extends Natively {

  @native def nullLit(): Long

  @native def fromString(value: String): Long

  @native def fromBool(value: Boolean): Long

  @native def fromInt(value: Int): Long

  @native def fromLong(value: Long): Long

  @native def fromFloat(value: Float): Long

  @native def fromDouble(value: Double): Long

  @native def fromDate(value: String): Long

  @native def fromTime(value: String): Long

  @native def fromDateTime(value: String): Long

}
