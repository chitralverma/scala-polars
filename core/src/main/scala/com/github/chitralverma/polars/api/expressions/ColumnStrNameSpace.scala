package com.github.chitralverma.polars.api.expressions

import com.github.chitralverma.polars.internal.jni.expressions.column_expr

/** Namespace for string-related expressions.
  *
  * In polars, string expressions are grouped under the `.str` namespace (e.g.
  * `col("x").str.to_uppercase`). This class provides the same scoped syntax on JVM columns,
  * mirroring the upstream polars API.
  */
class ColumnStrNameSpace private[polars] (private val parent: Expression) {

  /** Modify strings to their uppercase equivalent. */
  def to_uppercase: Column = {
    parent.checkClosed()
    Column.withPtr(column_expr.to_uppercase(parent.ptr))
  }
}
