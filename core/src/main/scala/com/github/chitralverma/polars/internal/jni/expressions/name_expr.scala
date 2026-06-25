package com.github.chitralverma.polars.internal.jni.expressions

import com.github.chitralverma.polars.internal.jni.Natively

private[polars] object name_expr extends Natively {

  @native def keep(ptr: Long): Long

  @native def prefix(ptr: Long, prefix: String): Long

  @native def suffix(ptr: Long, suffix: String): Long

  @native def to_uppercase(ptr: Long): Long

  @native def to_lowercase(ptr: Long): Long

}
