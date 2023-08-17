package org.polars.scala.polars.internal.jni

private[polars] object lazy_frame extends Natively {

  @native def concatLazyFrames(ptrs: Array[Long], reChunk: Boolean, parallel: Boolean): Long

  @native def schemaString(ptr: Long): String

  @native def selectFromStrings(ptr: Long, cols: Array[String]): Long

  @native def selectFromExprs(ptr: Long, exprs: Array[Long]): Long

  @native def filterFromExprs(ldfPtr: Long, exprPtr: Long): Long

  @native def limit(ptr: Long, n: Long): Long

  @native def tail(ptr: Long, n: Long): Long

  @native def drop(ptr: Long, cols: Array[String]): Long

  @native def rename(ptr: Long, mapping: java.util.Map[String, String]): Long

  @native def sortFromExprs(
      ldfPtr: Long,
      exprPtrs: Array[Long],
      nullLast: Boolean,
      maintainOrder: Boolean
  ): Long

  @native def withColumn(ldfPtr: Long, name: String, exprPtr: Long): Long

  @native def unique(ptr: Long, subset: Array[String], keep: String, maintainOrder: Boolean): Long

  @native def explain(ptr: Long, optimized: Boolean): String

  @native def cache(ptr: Long): Long

  @native def collect(ptr: Long): Long

  @native def optimization_toggle(
      ptr: Long,
      typeCoercion: Boolean,
      predicatePushdown: Boolean,
      projectionPushdown: Boolean,
      simplifyExpr: Boolean,
      slicePushdown: Boolean,
      commSubplanElim: Boolean,
      commSubexprElim: Boolean,
      streaming: Boolean
  ): Long

}
