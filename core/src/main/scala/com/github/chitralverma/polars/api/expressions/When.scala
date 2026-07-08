package com.github.chitralverma.polars.api.expressions

import com.github.chitralverma.polars.functions.lit
import com.github.chitralverma.polars.internal.jni.expressions.{column_expr, functions_expr}

/** A `when / otherwise` conditional expression, built one branch at a time.
  *
  * Created by [[com.github.chitralverma.polars.functions.when]] with a condition and the value to
  * use where that condition is true. Add more branches with [[when]], and set a default for
  * unmatched rows with [[otherwise]].
  *
  * This is itself a valid [[Expression]]: when used without an explicit [[otherwise]], rows that
  * match no condition evaluate to `null`. So a bare `when(...)` can be passed straight to
  * `select` or `withColumn`.
  *
  * The value from the first condition that is true is picked. If no condition is true, the
  * `otherwise` value (or `null`) is used.
  *
  * @note
  *   All branches are evaluated in parallel and filtered afterwards, so every value expression
  *   must be valid on its own, independent of the conditions.
  * @note
  *   The output column name is taken from the first branch.
  */
class When private[polars] (private val branches: List[(Expression, Any)])
    extends Expression(When.buildWithNullDefault(branches)) {

  /** Add another condition and its value to the chain.
    *
    * The condition is evaluated only for rows not matched by an earlier branch.
    *
    * @param condition
    *   the next condition
    * @param value
    *   the value for rows where this condition is true; an [[Expression]] is used directly, any
    *   other value is treated as a literal
    * @return
    *   a [[When]] which may be extended further or finalised with [[otherwise]]
    */
  def when(condition: Expression, value: Any): When =
    new When(branches :+ (condition -> value))

  /** Set the default value for rows where no condition matched.
    *
    * @param value
    *   the default value; an [[Expression]] is used directly, any other value is treated as a
    *   literal
    * @return
    *   the finalised conditional [[Column]]
    */
  def otherwise(value: Any): Column =
    Column.withPtr(When.build(branches, value))

}

private[polars] object When {

  /** Fold the branches into a single nested ternary pointer, ending with `default`.
    *
    * Branch values (and `default`) are materialised with `lit` here: an [[Expression]] is used as
    * is, any other value becomes a literal. Branches are combined last-in-first-out so that
    * earlier conditions take precedence.
    *
    * Every native handle allocated by this fold is released before returning: the intermediate
    * ternary handles (superseded as the accumulator advances) and any literal created for a value
    * or the default. The final returned handle, and any [[Expression]] the caller supplied
    * directly, are left intact.
    */
  private[polars] def build(branches: List[(Expression, Any)], default: Any): Long = {
    val defaultLit = lit(default)
    var acc = defaultLit.ptr
    var accIsIntermediate = false
    branches.reverse.foreach { case (condition, value) =>
      val valueLit = lit(value)
      try {
        val next = functions_expr.ternaryExpr(condition.ptr, valueLit.ptr, acc)
        if (accIsIntermediate) column_expr.free(acc)
        acc = next
        accIsIntermediate = true
      } finally if (valueLit ne value.asInstanceOf[AnyRef]) valueLit.close()
    }
    // The default is only cloned into a ternary when at least one branch was folded. If no branch
    // was folded, `acc` still is the default's own handle and must be returned as is.
    if (accIsIntermediate && (defaultLit ne default.asInstanceOf[AnyRef])) defaultLit.close()
    acc
  }

  /** Fold the branches with an implicit `null` default. */
  private[polars] def buildWithNullDefault(branches: List[(Expression, Any)]): Long =
    build(branches, null)

}
