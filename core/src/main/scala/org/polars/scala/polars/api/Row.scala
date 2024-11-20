package org.polars.scala.polars.api

import scala.jdk.CollectionConverters._

import org.polars.scala.polars.api.types.Schema
import org.polars.scala.polars.internal.jni.row
import org.polars.scala.polars.jsonMapper

class RowIterator private (private[polars] val ptr: Long) {

  private[polars] def lazyIterator(nRows: Long): Iterator[Row] = new Iterator[Row] {
    private val iteratorPtr = row.createIterator(ptr, nRows)
    private val schema = {
      val schemaString = row.schemaString(iteratorPtr)
      Schema.from(schemaString)
    }

    private var nextValue: Option[Array[Object]] = fetchNext()

    private def fetchNext(): Option[Array[Object]] = {
      val value = row.advanceIterator(iteratorPtr)
      Option(value)
    }

    override def hasNext: Boolean = nextValue.isDefined

    override def next(): Row = {
      val arr = nextValue.getOrElse(throw new NoSuchElementException("End of iterator"))
      nextValue = fetchNext()
      Row.fromObjects(arr, schema)
    }
  }
}

object RowIterator {

  private[polars] def withPtr(ptr: Long) = new RowIterator(ptr)
}

class Row private (private[polars] val arr: Array[Object], schema: Schema) {

  /** Returns the Schema for the row. This yields same value as [[DataFrame.schema]] */
  def getSchema: Schema = schema

  /** Returns the index of a given field name. */
  def fieldIndex(name: String): Int = schema.getFieldNames.indexOf(name)

  /** Returns the value at position `i`. */
  def get(i: Int): Object = arr(i)

  /** Returns the value by column name. */
  def get(name: String): Object = arr(fieldIndex(name))

  /** Checks whether the value at position `i` is null. */
  def isNullAt(i: Int): Boolean = arr(i) == null

  /** Returns a [[java.util.Map]] consisting of column names and their values. */
  def toJMap: java.util.Map[String, Object] = toMap.asJava

  /** Returns a Map consisting of column names and their values. */
  def toMap: Map[String, Object] = schema.getFieldNames.zip(toArray).toMap

  /** Return an Array of objects representing the row. */
  def toArray: Array[Object] = arr

  /** Return an Array of objects representing the row. */
  def toJsonString: String =
    jsonMapper.writeValueAsString(toMap)

  /** Return a scala Sequence of objects representing the row. */
  def toSeq: Seq[Object] = toArray.toSeq

  override def toString: String = toSeq.mkString("[", ", ", "]")

}
object Row {

  private[polars] def fromObjects(arr: Array[Object], schema: Schema) = new Row(arr, schema)
}
