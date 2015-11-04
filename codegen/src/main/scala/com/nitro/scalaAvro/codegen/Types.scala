package com.nitro.scalaAvro.codegen

import org.apache.avro.Schema.Type

object Types {

  val GenericRecordInterface = "org.apache.avro.generic.GenericRecord"
  val GenericRecordBase = "org.apache.avro.generic.GenericData.Record"

  val GenericEnumInterface = "org.apache.avro.generic.GenericEnumSymbol"
  val GenericEnumBase = "org.apache.avro.generic.GenericData.EnumSymbol"

  val GenericArrayInterface = "org.apache.avro.generic.GenericArray"
  val GenericArrayBase = "org.apache.avro.generic.GenericData.Array"

  //java type
  val GenericMap = "java.util.HashMap"

  val Utf8 = "org.apache.avro.util.Utf8"

  case class TypeInfo(scalaTypeName: String)

  private val TYPES = Map(
    Type.STRING -> TypeInfo("String"),
    Type.BYTES -> TypeInfo("java.nio.ByteBuffer"),
    Type.INT -> TypeInfo("Int"),
    Type.LONG -> TypeInfo("Long"),
    Type.FLOAT -> TypeInfo("Float"),
    Type.DOUBLE -> TypeInfo("Double"),
    Type.BOOLEAN -> TypeInfo("Boolean")
  )

  def scalaTypeName(t: Type): Option[String] = {
    TYPES.get(t).map(_.scalaTypeName)
  }

}
