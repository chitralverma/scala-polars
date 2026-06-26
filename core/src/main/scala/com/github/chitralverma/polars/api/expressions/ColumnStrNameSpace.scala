package com.github.chitralverma.polars.api.expressions

import com.github.chitralverma.polars.internal.jni.expressions.column_expr

/** Namespace for string-related expressions.
  *
  * In Polars, string expressions are grouped under the `.str` namespace (e.g.
  * `col("x").str.toUppercase`). This class provides the same scoped syntax on JVM columns,
  * mirroring the upstream polars API.
  */
class ColumnStrNameSpace private[polars] (private val parent: Expression) {

  /** Modify strings to their uppercase equivalent. */
  def toUppercase: Column = {
    parent.checkClosed()
    Column.withPtr(column_expr.to_uppercase(parent.ptr))
  }
}
