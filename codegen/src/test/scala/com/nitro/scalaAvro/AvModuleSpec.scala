package com.nitro.scalaAvro

import org.scalatest._
import spray.json._
import PartialAvroJsonProtocol._
import scala.Vector
import language.reflectiveCalls

class AvModuleSpec extends PropSpec with Matchers {
  def bounce(avsc: AvSchema) = avsc.toJson.toString.parseJson.convertTo[AvSchema]
  
  val partials = new {
    val wheel = """{ "type": "record", "name": "Wheel", "fields": [{ "name": "size", "type": "float" }] }"""
    val car = """{ "type": "record", "name": "Car", "fields": [{ "name": "wheels", "type": { "type": "array", "items": "Wheel"} }] }"""
    val carAndWheel = """{ "type": "record", "name": "Car", "fields": [{ "name": "wheels", "type": { "type": "array", "items": { "type": "record", "name": "Wheel", "fields": [{ "name": "size", "type": "float" }] }} }] }"""
  }

  property(s"Reads/Writes null") {
    AvNull shouldBe "\"null\"".parseJson.convertTo[AvSchema]
    AvNull.asInstanceOf[AvSchema].toJson.toString shouldBe "\"null\""
  }
  property(s"Reads/Writes boolean") {
    AvBoolean shouldBe "\"boolean\"".parseJson.convertTo[AvSchema]
    AvBoolean.asInstanceOf[AvSchema].toJson.toString shouldBe "\"boolean\""
  }
  property(s"Reads/Writes int") {
    AvInt shouldBe "\"int\"".parseJson.convertTo[AvSchema]
    AvInt.asInstanceOf[AvSchema].toJson.toString shouldBe "\"int\""
  }
  property(s"Reads/Writes long") {
    AvLong shouldBe "\"long\"".parseJson.convertTo[AvSchema]
    AvLong.asInstanceOf[AvSchema].toJson.toString shouldBe "\"long\""
  }
  property(s"Reads/Writes float") {
    AvFloat shouldBe "\"float\"".parseJson.convertTo[AvSchema]
    AvFloat.asInstanceOf[AvSchema].toJson.toString shouldBe "\"float\""
  }
  property(s"Reads/Writes double") {
    AvDouble shouldBe "\"double\"".parseJson.convertTo[AvSchema]
    AvDouble.asInstanceOf[AvSchema].toJson.toString shouldBe "\"double\""
  }
  property(s"Reads/Writes bytes") {
    AvBytes shouldBe "\"bytes\"".parseJson.convertTo[AvSchema]
    AvBytes.asInstanceOf[AvSchema].toJson.toString shouldBe "\"bytes\""
  }
  property(s"Reads/Writes string") {
    AvString shouldBe "\"string\"".parseJson.convertTo[AvSchema]
    AvString.asInstanceOf[AvSchema].toJson.toString shouldBe "\"string\""
  }
  property(s"Reads/Writes record") {
  	val recordStr = """{
	  "namespace": "com.nitro.example.messages",
	  "type": "record",
	  "name": "Point",
	  "fields": [
	    {"name": "name", "type": ["null", "string"]},
	    {"name": "x", "type": "int"},
	    {"name": "y", "type": "int"}
	  ]
	}"""
	val avRecord = AvRecord(
      namespace = Some("com.nitro.example.messages"),
      name = "Point",
      fields = Seq(
        AvField(name = "name", `type` = AvUnion(AvNull, AvString)),
        AvField(name = "x", `type` = AvInt),
        AvField(name = "y", `type` = AvInt)
      )
	)

    recordStr.parseJson.convertTo[AvSchema] shouldBe avRecord
    avRecord shouldBe bounce(avRecord)
  }
  property(s"Reads/Writes array") {
  	val arrStr = """{"type":"array", "items": "long"}"""
  	AvArray(AvLong) shouldBe arrStr.parseJson.convertTo[AvSchema]
  	AvArray(AvLong) shouldBe bounce(AvArray(AvLong))
  }
  property(s"Reads/Writes map") {
  	val mapStr = """{"type":"map", "values": "long"}"""
  	AvMap(AvLong) shouldBe mapStr.parseJson.convertTo[AvSchema]
  	AvMap(AvLong) shouldBe bounce(AvMap(AvLong))
  }
  property(s"Reads/Writes enum") {
  	val enumStr = """{
      "namespace": "com.nitro.example.messages",
      "type": "enum",
      "name": "MyEnum",
      "symbols" : ["Foo", "Bar", "Baz"]
    }"""
    val avEnum = AvEnum(namespace = Some("com.nitro.example.messages"), name = "MyEnum", symbols = Vector("Foo", "Bar", "Baz"))
    enumStr.parseJson.convertTo[AvSchema] shouldBe avEnum
    avEnum shouldBe bounce(avEnum)
  }
  property(s"Reads/Writes union") {
    val unionStr = """["null", "string", { "type": "map", "values": "long"}]"""
    val avUnion = AvUnion(AvNull, AvString, AvMap(AvLong))
    unionStr.parseJson.convertTo[AvSchema] shouldBe avUnion
    avUnion shouldBe bounce(avUnion)
  }
  property(s"Reads/Writes fixed") {
  	val fixedStr = """{ "type": "fixed", "size": 16, "name": "md5", "namespace": "com.nitro.example.messages"}"""
    val avFixed = AvFixed(size = 16, name = "md5", namespace = Some("com.nitro.example.messages"))
    fixedStr.parseJson.convertTo[AvSchema] shouldBe avFixed
    avFixed shouldBe bounce(avFixed)
  }
  
  property(s"Reads interdependent partial Avro strings.") {
    val avModule = AvModule.fromStringPartials(Seq(partials.car, partials.wheel))
  }
  
  property(s"Allows identical redefinitions.") {
    val avModule = AvModule.fromStringPartials(Seq(partials.car, partials.wheel, partials.carAndWheel))
  }
  
  property(s"Fails if a redefinition is not identical to a prior definition.") {
    //size should be float not double
    val conflictingWheel = """{ "type": "record", "name": "Wheel", "fields": [{ "name": "size", "type": "double" }] }"""
    val abc = an [Exception] should be thrownBy AvModule.fromStringPartials(Seq(conflictingWheel, partials.carAndWheel))
  }
  
  property(s"Fails if no definition is found for a reference across all partials.") {
    //Window is not defined
    val door = """{ "type": "record", "name": "Door", "fields": [{ "name": "window", "type": "Window" }] }"""
    val abc = an [Exception] should be thrownBy AvModule.fromStringPartials(Seq(door, partials.carAndWheel))
  }
  
  property(s"Builds canonical schema compatible with org.apache.avro.Schema.Parser") {
    val parser = new org.apache.avro.Schema.Parser()
    val canonical = parser.parse(partials.carAndWheel).toString.parseJson.convertTo[AvSchema]
    val key = AvReference(Some(""), "Car")
    val fromPartials = AvModule.fromStringPartials(Seq(partials.car, partials.wheel)).lookup(key).get.toString
    fromPartials.parseJson.convertTo[AvSchema] shouldBe canonical
  }
}