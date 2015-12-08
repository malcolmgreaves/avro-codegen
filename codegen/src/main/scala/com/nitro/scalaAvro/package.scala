package com.nitro
import spray.json._
import org.apache.avro.Schema

/**
 * @author ebiggs
 */

package object scalaAvro {
  implicit def avSchemaToEither(avsc: AvSchema): Either[AvSchema, AvReference] = Left(avsc)
  implicit def avReferenceToEither(avRef: AvReference): Either[AvSchema, AvReference] = Right(avRef)
}

package scalaAvro {
  
  case class AvReference(namespace: Option[String], name: String)
    
  trait AvReferable {
    def namespace: Option[String]
    def name: String
    def reference: AvReference = AvReference(namespace, name)
  }
  
  trait AvPrimitive extends AvReferable {
    def namespace = None
    def typeName: String
    def name = typeName
    def children: Seq[AvSchema] = Seq()
  }
  
  object AvPrimitive {
    val all: Seq[AvPrimitive] = Seq(AvNull, AvBoolean, AvInt, AvLong, AvFloat, AvDouble, AvBytes, AvString)
  }
  
  trait AvComplex {
    def typeName: String
  }
  
  sealed abstract class AvSchema(val typeName: String) { self =>
    //def toSchema: Schema = schemaParser.parse(this.toJson.toString)
    
    def referenceOpt: Option[AvReference] = self match {
      case r: AvReferable => Some(r.reference)
      case _ => None
    }
    
    def children: Seq[AvSchema]
  }
  
  case object AvNull extends AvSchema("null") with AvPrimitive
  case object AvBoolean extends AvSchema("boolean") with AvPrimitive
  case object AvInt extends AvSchema("int") with AvPrimitive
  case object AvLong extends AvSchema("long") with AvPrimitive
  case object AvFloat extends AvSchema("float") with AvPrimitive
  case object AvDouble extends AvSchema("double") with AvPrimitive
  case object AvBytes extends AvSchema("bytes") with AvPrimitive
  case object AvString extends AvSchema("string") with AvPrimitive
  
  sealed abstract class AvOrder
  case object AvOrderAscending extends AvOrder
  case object AvOrderDescending extends AvOrder
  case object AvOrderIgnore extends AvOrder
  
  case class AvField(
    name: String, 
    doc: Option[String] = None,
    `type`: Either[AvSchema, AvReference],
    default: Option[JsValue] = None,
    order: Option[AvOrder] = None,
    aliases: Seq[String] = Seq()
   )
  
  case class AvRecord(
    name: String, 
    namespace: Option[String] = None,
    doc: Option[String] = None, 
    aliases: Seq[String] = Seq(), 
    fields: Seq[AvField] = Seq(),
    meta: Seq[(String, JsValue)] = Seq()
  ) extends AvSchema(AvRecord.typeName) with AvComplex with AvReferable {
    def children = fields.map(_.`type`).collect { case (Left(avsc)) => avsc }
  }
  
  object AvRecord extends AvComplex { val typeName: String = "record" } 
  
  case class AvArray(items: Either[AvSchema, AvReference]) extends AvSchema(AvArray.typeName) with AvComplex {
    def children = Seq(items).collect { case (Left(avsc)) => avsc }
  }
  
  object AvArray extends AvComplex { val typeName: String = "array" }
  
  case class AvUnion(types: Either[AvSchema, AvReference]*) extends AvSchema(AvUnion.typeName) with AvComplex {
    def children = types.collect { case (Left(avsc)) => avsc }
  }
  
  object AvUnion extends AvComplex { val typeName: String = "union" }
  
  case class AvEnum(
    name: String,
    namespace: Option[String] = None,
    doc: Option[String] = None, 
    symbols: Seq[String]
  ) extends AvSchema(AvEnum.typeName) with AvComplex with AvReferable {
    def children: Seq[AvSchema] = Seq()
  }
  
  object AvEnum extends AvComplex { val typeName: String = "enum" }
  
  case class AvMap(values: Either[AvSchema, AvReference]) extends AvSchema(AvMap.typeName) with AvComplex {
    def children = Seq(values).collect { case (Left(avsc)) => avsc }
  }
  
  case object AvMap extends AvComplex { val typeName: String = "map" }
  
  case class AvFixed(
    name: String,
    namespace: Option[String] = None,
    aliases: Seq[String] = Seq(), 
    size: Int
  ) extends AvSchema(AvFixed.typeName) with AvComplex with AvReferable {
    def children: Seq[AvSchema] = Seq()
  }
  
  case object AvFixed extends AvComplex { val typeName: String = "fixed" }
}