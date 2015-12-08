package com.nitro
import spray.json._
import org.apache.avro.Schema
import scalaAvro.PartialAvroJsonProtocol._

/**
 * @author ebiggs
 */

package object scalaAvro {
  private[scalaAvro] val schemaParser = new Schema.Parser
  implicit def avSchemaToEither(avsc: AvSchema): Either[AvSchema, AvReference] = Left(avsc)
  implicit def avReferenceToEither(avRef: AvReference): Either[AvSchema, AvReference] = Right(avRef)
  implicit def pimpSchema(schema: Schema) = new PimpedSchema(schema)
}

package scalaAvro {
  private[scalaAvro] class PimpedSchema(schema: Schema) {
    def toAvSchema: AvSchema = schema.toString.parseJson.convertTo[AvSchema]
  }
  
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
    def children: Seq[Either[AvSchema, AvReference]] = Seq()
  }
  
  object AvPrimitive {
    val all: Seq[AvPrimitive] = Seq(AvNull, AvBoolean, AvInt, AvLong, AvFloat, AvDouble, AvBytes, AvString)
  }
  
  trait AvComplex {
    def typeName: String
  }
  
  sealed abstract class AvSchema(val typeName: String) { self =>
    def toSchema: Schema = schemaParser.parse(this.toJson.toString)
    
    def referenceOpt: Option[AvReference] = self match {
      case r: AvReferable => Some(r.reference)
      case _ => None
    }
    
    def children: Seq[Either[AvSchema, AvReference]]
    
    /*
     * Traverse the tree returning all Referable Schemas and References encountered
     * along the way.
     */
    def traverse: (Seq[AvSchema with AvReferable], Seq[AvReference]) = {
      val (childDefs, childRefs) = separate(children)
      val (recursiveDefs, recursiveRefs) = childDefs.map(_.traverse).unzip
      val defs = self match {
        case x: AvSchema with AvReferable => recursiveDefs.flatten ++ Seq(x)
        case _ => recursiveDefs.flatten
      }
      val refs = (recursiveRefs.flatten ++ childRefs)
      (defs, refs.distinct)
    }
    
    private[this] def separate[A, B](seq: Seq[Either[A, B]]): (Seq[A], Seq[B]) = {
      val (lefts, rights) = seq.partition(_.isLeft)
      (lefts.map(_.left.get), rights.map(_.right.get))
    }

    def copyWithReferencesOnly: AvSchema = {
      def forceRef(either: Either[AvSchema, AvReference]) = either match {
        case Left(r: AvReferable) => Right(r.reference)
        case x => x
      }
      self match {
        case av: AvPrimitive => av
        case av: AvEnum => av
        case av: AvFixed => av
        case av: AvArray => av.copy(items = forceRef(av.items))
        case av: AvMap => av.copy(values = forceRef(av.values))
        case av: AvUnion => AvUnion(av.types.map(forceRef):_*)
        case av: AvRecord => av.copy(fields = av.fields.map(field => field.copy(`type` = forceRef(field.`type`))))
      }
    }
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
    def children = fields.map(_.`type`)
  }
  
  object AvRecord extends AvComplex { val typeName: String = "record" } 
  
  case class AvArray(items: Either[AvSchema, AvReference]) extends AvSchema(AvArray.typeName) with AvComplex {
    def children = Seq(items)
  }
  
  object AvArray extends AvComplex { val typeName: String = "array" }
  
  case class AvUnion(types: Either[AvSchema, AvReference]*) extends AvSchema(AvUnion.typeName) with AvComplex {
    def children = types
  }
  
  object AvUnion extends AvComplex { val typeName: String = "union" }
  
  case class AvEnum(
    name: String,
    namespace: Option[String] = None,
    doc: Option[String] = None, 
    symbols: Seq[String]
  ) extends AvSchema(AvEnum.typeName) with AvComplex with AvReferable {
    def children: Seq[Either[AvSchema, AvReference]] = Seq()
  }
  
  object AvEnum extends AvComplex { val typeName: String = "enum" }
  
  case class AvMap(values: Either[AvSchema, AvReference]) extends AvSchema(AvMap.typeName) with AvComplex {
    def children = Seq(values)
  }
  
  case object AvMap extends AvComplex { val typeName: String = "map" }
  
  case class AvFixed(
    name: String,
    namespace: Option[String] = None,
    aliases: Seq[String] = Seq(), 
    size: Int
  ) extends AvSchema(AvFixed.typeName) with AvComplex with AvReferable {
    def children: Seq[Either[AvSchema, AvReference]] = Seq()
  }
  
  case object AvFixed extends AvComplex { val typeName: String = "fixed" }
}