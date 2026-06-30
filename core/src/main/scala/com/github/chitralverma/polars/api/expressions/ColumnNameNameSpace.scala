package com.github.chitralverma.polars.api.expressions

import com.github.chitralverma.polars.internal.jni.expressions.name_expr

/** Namespace for expression name manipulation.
  *
  * In Polars, name modifications (renaming, prefixing, suffixing, etc.) on expressions are
  * grouped under the `.name` namespace (e.g. `col("x").name.suffix("_new")`). This class provides
  * the same scoped syntax on JVM columns, mirroring the upstream Polars API.
  */
class ColumnNameNameSpace private[polars] (private val parent: Expression) {

  /** Keep the original name of the input column. */
  def keep: Column = {
    parent.checkClosed()
    Column.withPtr(name_expr.keep(parent.ptr))
  }

  /** Add a prefix to the name of the input column.
    *
    * @param prefix
    *   the string prefix to prepend
    * @return
    *   a Column expression with the prefixed name
    */
  def prefix(prefix: String): Column = {
    parent.checkClosed()
    Column.withPtr(name_expr.prefix(parent.ptr, prefix))
  }

  /** Add a suffix to the name of the input column.
    *
    * @param suffix
    *   the string suffix to append
    * @return
    *   a Column expression with the suffixed name
    */
  def suffix(suffix: String): Column = {
    parent.checkClosed()
    Column.withPtr(name_expr.suffix(parent.ptr, suffix))
  }

  /** Convert the name of the input column to uppercase. */
  def toUppercase: Column = {
    parent.checkClosed()
    Column.withPtr(name_expr.toUppercase(parent.ptr))
  }

  /** Convert the name of the input column to lowercase. */
  def toLowercase: Column = {
    parent.checkClosed()
    Column.withPtr(name_expr.toLowercase(parent.ptr))
  }

}
