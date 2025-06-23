package com.github.chitralverma.polars.api

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Try

import com.github.chitralverma.polars.api.Row.assertDataType
import com.github.chitralverma.polars.api.types._
import com.github.chitralverma.polars.internal.jni.row
import com.github.chitralverma.polars.jsonMapper

class RowIterator private (private[polars] val ptr: Long) {

  private[polars] def lazyIterator(nRows: Long): Iterator[Row] = new Iterator[Row] {
    private val iteratorPtr = row.createIterator(ptr, nRows)
    private val schema = {
      val schemaString = row.schemaString(iteratorPtr)
      Schema.fromString(schemaString)
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

  assert(schema != null, "Schema of a Row cannot be null")

  /** Returns the Schema for the row. This yields same value as [[DataFrame.schema]] */
  def getSchema: Schema = schema

  /** Returns the data type of field at position `i`. */
  def getDataType(i: Int): DataType = schema.getFields(i).dataType

  /** Returns the data type of field by column `name`. */
  def getDataType(name: String): DataType = getDataType(fieldIndex(name))

  /** Returns the index of a given field `name`. */
  def fieldIndex(name: String): Int = schema.getFieldNames.indexOf(name)

  /** Returns the value at position `i`. */
  def get(i: Int): Object = arr(i)

  /** Returns the value by column `name`. */
  def get(name: String): Object = arr(fieldIndex(name))

  /** Returns the value at position `i` as the provided generic type.
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getAs[T: ClassTag](i: Int): T = {
    val v = get(i)
    val clazz = implicitly[ClassTag[T]].runtimeClass
    if (clazz.isInstance(v)) v.asInstanceOf[T]
    else throw new ClassCastException(s"Unable to cast value `$v` as `${clazz.getCanonicalName}`")
  }

  /** Returns the value by column `name` as the provided generic type.
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getAs[T: ClassTag](name: String): T =
    getAs[T](fieldIndex(name))

  /** Returns the value at position `i` as the per the provided Class.
    *
    * @param cls
    *   Class of output value
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getAs[T](i: Int, cls: Class[T]): T = {
    val v = get(i)
    if (cls.isInstance(v)) cls.cast(v)
    else throw new ClassCastException(s"Unable to cast value `$v` as `${cls.getCanonicalName}`")
  }

  /** Returns the value by column `name` as the per the provided class.
    *
    * @param cls
    *   Class of output value
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getAs[T](name: String, cls: Class[T]): T =
    getAs[T](fieldIndex(name), cls)

  /** Returns the value at position `i` as Boolean
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getBoolean(i: Int): Boolean = {
    assertDataType(getDataType(i), BooleanType)
    getAs[java.lang.Boolean](i).booleanValue()
  }

  /** Returns the value by column `name` as Boolean
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getBoolean(name: String): Boolean =
    getBoolean(fieldIndex(name))

  /** Returns the value at position `i` as Int
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getInt(i: Int): Int = {
    assertDataType(getDataType(i), IntegerType)
    getAs[java.lang.Integer](i).intValue()
  }

  /** Returns the value by column `name` as Int
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getInt(name: String): Int =
    getInt(fieldIndex(name))

  /** Returns the value at position `i` as Long
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getLong(i: Int): Long = {
    assertDataType(getDataType(i), LongType)
    getAs[java.lang.Long](i).longValue()
  }

  /** Returns the value by column `name` as Long
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getLong(name: String): Long = getLong(fieldIndex(name))

  /** Returns the value at position `i` as Float
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getFloat(i: Int): Float = {
    assertDataType(getDataType(i), FloatType)
    getAs[java.lang.Float](i).floatValue()
  }

  /** Returns the value by column `name` as Float
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getFloat(name: String): Float = getFloat(fieldIndex(name))

  /** Returns the value at position `i` as Double
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getDouble(i: Int): Double = {
    assertDataType(getDataType(i), DoubleType)
    getAs[java.lang.Double](i).doubleValue()
  }

  /** Returns the value by column `name` as Double
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getDouble(name: String): Double = getDouble(fieldIndex(name))

  /** Returns the value at position `i` as [[java.time.LocalDate]]
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getDate(i: Int): java.time.LocalDate = {
    assertDataType(getDataType(i), DateType)
    getAs[java.time.LocalDate](i)
  }

  /** Returns the value by column `name` as [[java.time.LocalDate]]
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getDate(name: String): java.time.LocalDate = getDate(fieldIndex(name))

  /** Returns the value at position `i` as String
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getString(i: Int): String = {
    assertDataType(getDataType(i), StringType)
    getAs[String](i)
  }

  /** Returns the value by column `name` as String
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getString(name: String): String = getString(fieldIndex(name))

  /** Returns the value at position `i` as [[java.time.LocalTime]]
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getTime(i: Int): java.time.LocalTime = {
    getSchema.getFields(i).dataType match {
      case TimeType(_) =>
      case x => assertDataType(x, TimeType)
    }

    getAs[java.time.LocalTime](i)
  }

  /** Returns the value by column `name` as [[java.time.LocalTime]]
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getTime(name: String): java.time.LocalTime = getTime(fieldIndex(name))

  /** Returns the value at position `i` as [[java.time.ZonedDateTime]]
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getZonedDateTime(i: Int): java.time.ZonedDateTime = {
    val zoneId = getSchema.getFields(i).dataType match {
      case DateTimeType(_, tz) =>
        Try(java.time.ZoneId.of(tz)).getOrElse(java.time.ZoneId.systemDefault())
      case x =>
        assertDataType(x, DateTimeType)
        java.time.ZoneId.systemDefault()
    }

    val instant = getAs[java.time.Instant](i)
    java.time.ZonedDateTime.ofInstant(instant, zoneId)
  }

  /** Returns the value by column `name` as [[java.time.ZonedDateTime]]
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getZonedDateTime(name: String): java.time.ZonedDateTime = getZonedDateTime(fieldIndex(name))

  /** Returns the value at position `i` as [[java.util.List]]
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getJList[T: ClassTag](i: Int): java.util.List[T] = {
    getSchema.getFields(i).dataType match {
      case ListType(dt) => assertDataType(dt, DataType.typeToDataType[T]())
      case x => assertDataType(x, ListType)
    }

    getAs[java.util.List[T]](i)
  }

  /** Returns the value by column `name` as [[java.util.List]]
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getJList[T: ClassTag](name: String): java.util.List[T] = getJList[T](fieldIndex(name))

  /** Returns the value at position `i` as scala Sequence
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getSeq[T: ClassTag](i: Int): Seq[T] = getJList[T](i).asScala.toSeq

  /** Returns the value by column `name` as scala Sequence
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getSeq[T: ClassTag](name: String): Seq[T] = getSeq[T](fieldIndex(name))

  /** Returns the value(s) of struct column at position `i` as [[Row]]
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getStruct(i: Int): Row = {
    val s = getSchema.getFields(i).dataType match {
      case StructType(fields) =>
        Schema.fromFields(fields)
      case x =>
        assertDataType(x, StructType)
        null
    }

    val arr = getAs[java.util.HashMap[String, Object]](i).values().toArray

    Row.fromObjects(arr, s)
  }

  /** Returns the value(s) of struct column `name` as [[Row]]
    *
    * @throws java.lang.ClassCastException
    *   when data type does not match.
    */
  def getStruct(name: String): Row = getStruct(fieldIndex(name))

  /** Checks whether the value at position `i` is null. */
  def isNullAt(i: Int): Boolean = arr(i) == null

  /** Checks whether the value by column `name` is null. */
  def isNullAt(name: String): Boolean = isNullAt(fieldIndex(name))

  /** Returns a [[java.util.Map]] consisting of column names and their values. */
  def toJMap: java.util.Map[String, Object] = toMap.asJava

  /** Returns a Map consisting of column names and their values. */
  def toMap: Map[String, Object] = schema.getFieldNames.zip(toArray).toMap

  /** Return an Array of objects representing the row. */
  def toArray: Array[Object] = arr

  /** Return a scala Sequence of objects representing the row. */
  def toList: Seq[Object] = toArray.toSeq

  /** Return a [[java.util.List]] of objects representing the row. */
  def toJList: java.util.List[Object] = toList.asJava

  /** Return an Array of objects representing the row. */
  def toJsonString: String =
    toJsonString(false)

  /** Return an Array of objects representing the row.
    *
    * @param pretty
    *   flag to create pretty json
    */
  def toJsonString(pretty: Boolean): String =
    if (pretty) jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(toMap)
    else jsonMapper.writeValueAsString(toMap)

  override def toString: String = toJsonString

}
object Row {

  private def assertDataType(target: DataType, dt: DataType): Unit =
    assert(dt == target, s"Data Type mismatch, field is of `$target` but `$dt` was provided")

  def fromObjects(arr: Array[Object], schema: Schema) = new Row(arr, schema)
}
