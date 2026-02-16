package com.github.chitralverma.polars.api.types

import scala.jdk.CollectionConverters._
import scala.util.Try

import com.fasterxml.jackson.databind.JsonNode
import com.github.chitralverma.polars.jsonMapper

case class Field(name: String, dataType: DataType) {

  /** Borrowed from Apache Spark source to represent [[Field]] as a tree string. */
  private[polars] def buildFormattedString(prefix: String, buffer: StringBuffer): Unit = {
    buffer.append(s"$prefix-- $name: ${dataType.simpleName} \n")
    DataType.buildFormattedString(dataType, s"$prefix    |", buffer)
  }

}

class Schema {

  private var _fields: Array[Field] = _

  def getFields: Array[Field] = _fields

  def getFieldNames: Array[String] = _fields.map(f => f.name)

  def getField(i: Int): Option[Field] = Try(getFields(i)).toOption

  def getField(name: String, ignoreCase: Boolean = false): Option[Field] =
    getFields.find { field =>
      val fieldName = field.name
      if (ignoreCase) fieldName.equalsIgnoreCase(name)
      else fieldName.equals(name)
    }

  def getFieldIndex(name: String, ignoreCase: Boolean = false): Option[Int] =
    getField(name, ignoreCase).map(f => getFields.indexOf(f))

  override def toString: String = treeString

  private def setFields(fields: Array[Field]): Schema = {
    fields match {
      case f if f == null || f.isEmpty =>
        throw new IllegalArgumentException("Provided fields cannot be null or empty.")

      case _ =>
        _fields = fields
    }

    this
  }

  private def deserialize(json: String): Schema = {
    Try(jsonMapper.reader.readTree(json)).toOption match {
      case None =>
        throw new IllegalArgumentException("Provided schema string cannot be parsed as a JSON.")

      case Some(node: JsonNode) if node.hasNonNull("fields") =>
        val fields = node.get("fields").elements().asScala.toList
        _fields = fields
          .map(f => Field(f.get("name").textValue(), DataType.fromJson(f.get("dtype"))))
          .toArray

      case _ =>
        throw new IllegalArgumentException("Provided schema string is an invalid JSON.")
    }

    this
  }

  /** Borrowed from Apache Spark source to represent Schema as a tree string. */
  private[polars] def treeString: String = {
    val stringBuffer = new StringBuffer()
    stringBuffer.append("root\n")
    val prefix = " |"
    getFields.foreach(field => field.buildFormattedString(prefix, stringBuffer))

    stringBuffer.toString
  }

}

object Schema {
  def fromString(jsonString: String): Schema = new Schema().deserialize(jsonString)

  def fromFields(fields: Array[Field]): Schema = new Schema().setFields(fields)
}
