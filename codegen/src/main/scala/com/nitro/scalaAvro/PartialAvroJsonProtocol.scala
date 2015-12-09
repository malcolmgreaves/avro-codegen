package com.nitro.scalaAvro

import spray.json._
import scala.collection.immutable.ListMap

/**
 * @author ebiggs
 * 
 * This is a permissive Avro Schema Parser, that accommodates "Partial" Avro Schema definitions
 * which means types can be referenced without being defined, under the assumption they'll be 
 * defined at some later point and looked up.
 */
object PartialAvroJsonProtocol extends DefaultJsonProtocol {
  type JsFieldOpt = Option[(String, JsValue)]
  
  implicit object AvSchemaWriter extends JsonWriter[AvSchema] {
    def write(schema: AvSchema): JsValue = writeAvSchema("", schema)
    
    def writeEither(namespace: String, either: Either[AvSchema, AvReference]): JsValue = {
      either match {
        case Left(avsc) => writeAvSchema(namespace, avsc)
        case Right(ref) => writeAvReference(namespace, ref)
      }
    }
    
    def inheritNamespace(thisNamespace: Option[String], namespace: String): Option[String] = {
      thisNamespace match {
        case Some("") => None
        case Some(`namespace`) => None
        case x => x
      }
    }
    
    def writeAvReference(namespace: String, ref: AvReference): JsValue = {
      val thisRef = AvReference(inheritNamespace(ref.namespace, namespace), ref.name)
      JsString(thisRef.toString)
    }
    
    def writeAvPrimitive(namespace: String, primitive: AvPrimitive): JsValue = {
      JsString(primitive.typeName)
    }
    
    def writeAvSchema(namespace: String, schema: AvSchema): JsValue = {
      schema match {
        case av: AvPrimitive => writeAvPrimitive(namespace, av)
        case av: AvRecord => writeAvRecord(namespace, av)
        case av: AvArray => writeAvArray(namespace, av)
        case av: AvMap => writeAvMap(namespace, av)
        case av: AvEnum => writeAvEnum(namespace, av)
        case av: AvFixed => writeAvFixed(namespace, av)
        case av: AvUnion => writeAvUnion(namespace, av)
      }
    }
    
    private def makeJsObject(optFields: JsFieldOpt*): JsObject =
      makeJsObjectWithMeta(Seq(), optFields:_*)
      
    private def makeJsObjectWithMeta(meta: Seq[(String, JsValue)], optFields: JsFieldOpt*): JsObject = {
      val props = (optFields.collect { case Some(tuple) => tuple }) ++ meta
      
      JsObject(ListMap(props:_*))
    }
    
    def writeAvField(namespace: String, field: AvField): JsValue = {
      val name: JsFieldOpt = Some("name" -> JsString(field.name))
      
      val doc: JsFieldOpt = field.doc.map("doc" -> JsString(_))
      
      val json = writeEither(namespace, field.`type`)
      
      val `type`: JsFieldOpt = Some("type" -> json)
      
      val default = field.default.map("default" -> _)
      
      val order: JsFieldOpt = field.order.map {
        case AvOrderAscending => "order" -> JsString("ascending")
        case AvOrderDescending => "order" -> JsString("descending")
        case AvOrderIgnore => "order" -> JsString("ignore")
      }
      
      val aliases: JsFieldOpt = field.aliases.length match {
        case 0 => None
        case _ => Some("aliases" -> JsArray(field.aliases.map(JsString(_)).toVector))
      }

      makeJsObject(name, doc, `type`, default, order, aliases)
    }
    
    def writeAvRecord(namespace: String, record: AvRecord): JsValue = {
      val `type`: JsFieldOpt = Some("type" -> JsString(record.typeName))
      
      val name: JsFieldOpt = Some("name" -> JsString(record.name))
      
      val thisNamespace = inheritNamespace(record.namespace, namespace)
      val namespaceField :JsFieldOpt = thisNamespace.map("namespace" -> JsString(_))
      
      val doc: JsFieldOpt = record.doc.map("doc" -> JsString(_))
      
      val aliases: JsFieldOpt = record.aliases.length match {
        case 0 => None
        case _ => Some("aliases" -> JsArray(record.aliases.map(JsString(_)).toVector))
      }
      
      val processedFields = record.fields.map(writeAvField(thisNamespace.getOrElse(namespace), _))
      
      val fields = Some("fields" -> JsArray(processedFields.toVector))
      
      makeJsObjectWithMeta(record.meta, `type`, name, namespaceField, doc, aliases, fields)
    }
    
    def writeAvArray(namespace: String, array: AvArray): JsValue = {
      val `type`: JsFieldOpt = Some("type" -> JsString(array.typeName))
      
      val json = writeEither(namespace, array.items)
      val items: JsFieldOpt = Some("items" -> json)
      
      makeJsObject(`type`, items)
    }
    
    def writeAvMap(namespace: String, map: AvMap): JsValue = {
      val `type`: JsFieldOpt = Some("type" -> JsString(map.typeName))
      
      val json = writeEither(namespace, map.values)
      val values: JsFieldOpt = Some("values" -> json)
      
      makeJsObject(`type`, values)
    }
    
    def writeAvEnum(namespace: String, enum: AvEnum): JsValue = {
      val `type`: JsFieldOpt = Some("type" -> JsString(enum.typeName))
      
      val name: JsFieldOpt = Some("name" -> JsString(enum.name))
      
      val thisNamespace = inheritNamespace(enum.namespace, namespace)
      
      val namespaceField :JsFieldOpt = thisNamespace.map("namespace" -> JsString(_))
      
      val doc: JsFieldOpt = enum.doc.map("doc" -> JsString(_))
      
      val symbols: JsFieldOpt = Some("symbols" -> enum.symbols.toJson)
      
      makeJsObject(`type`, name, namespaceField, doc, symbols)
    }
    
    def writeAvFixed(namespace: String, fixed: AvFixed): JsValue = {
      val `type`: JsFieldOpt = Some("type" -> JsString(fixed.typeName))
      val name: JsFieldOpt = Some("name" -> JsString(fixed.name))
      val thisNamespace = inheritNamespace(fixed.namespace, namespace)
      val namespaceField :JsFieldOpt = thisNamespace.map("namespace" -> JsString(_))
      val size: JsFieldOpt = Some("size" -> JsNumber(fixed.size))
      val aliases: JsFieldOpt = fixed.aliases.length match {
        case 0 => None
        case _ => Some("aliases" -> JsArray(fixed.aliases.map(JsString(_)).toVector))
      }
      
      makeJsObject(`type`, name, namespaceField, aliases, size)
    }
    
    def writeAvUnion(namespace: String, union: AvUnion): JsValue = {
      val jsons = union.types.map(writeEither(namespace, _))
      JsArray(jsons:_*)
    }
  }
  
  implicit object AvSchemaReader extends JsonReader[AvSchema] {
    
    val primitiveNames = AvPrimitive.all.map(_.typeName)
    
    def read(value: JsValue): AvSchema = readAvSchema("", value)
    
    def readAvSchema(namespace: String, value: JsValue): AvSchema = {
      value match {
        case JsString(str) =>
          if (primitiveNames.contains(str)) {
            readAvPrimitive(str)
          } else {
            throw new DeserializationException("Unknown Avro Primitive: " + str)
          }
        case JsObject(fields) =>
          fields.get("type") match {
            case Some(JsString(AvRecord.typeName)) => readAvRecord(namespace, fields)
            case Some(JsString(AvMap.typeName)) => readAvMap(namespace, fields)
            case Some(JsString(AvArray.typeName)) => readAvArray(namespace, fields)
            case Some(JsString(AvEnum.typeName)) => readAvEnum(namespace, fields)
            case Some(JsString(AvFixed.typeName)) => readAvFixed(namespace, fields)
            case Some(js) => throw new DeserializationException(
              s"""Expected "type" to be one of "${AvRecord.typeName}", "${AvRecord.typeName}", "${AvMap.typeName}", "${AvArray.typeName}", "${AvFixed.typeName}". Found: ${js.toString} """
            )
            case None => throw new DeserializationException("Avro Schema JsObject must define a \"type\" field.")
          }
        case JsArray(types) => readAvUnion(namespace, types)

        case js => throw new DeserializationException("Avro Schema must be a JsString, JsObject, or JsArray, found: " + js.toString)
      }
    }
    
    def readEither(namespace:String, value: JsValue): Either[AvSchema, AvReference] = value match {
      case JsString(str) if !primitiveNames.contains(str) =>
        readAvReference(namespace, str)
      case x => readAvSchema(namespace, x)
    }
    
    def readAvReference(namespace: String, str: String): AvReference = {
      val pieces = str.split("\\.").toList.reverse
      val name = pieces.head
      val nextNamespace = pieces.tail.reverse match {
        case Nil => None
        case xs => Some(xs.mkString("."))
      }
      
      AvReference(Some(nextNamespace.getOrElse(namespace)), name)
    }
      
    def readAvPrimitive(primitiveName: String): AvSchema = primitiveName match {
      case AvNull.typeName => AvNull
      case AvBoolean.typeName => AvBoolean
      case AvInt.typeName => AvInt
      case AvLong.typeName => AvLong
      case AvFloat.typeName => AvFloat
      case AvDouble.typeName => AvDouble
      case AvBytes.typeName => AvBytes
      case AvString.typeName => AvString
      case _ => throw new DeserializationException("Unknown Avro Primitive: " + primitiveName)
    }
    
    def readAvField(namespace: String, value: JsValue): AvField = {
      val obj = value.asJsObject("AvField must be a JsObject.").fields
      val name = obj("name").convertTo[String]
      val doc = obj.get("doc").map(_.convertTo[String])
      val `type` = readEither(namespace, obj("type"))
      val default = obj.get("default")
      val order = obj.get("order").map { 
        case JsString("ascending") => AvOrderAscending
        case JsString("descending") => AvOrderDescending
        case JsString("ignore") => AvOrderIgnore
        case x => throw new DeserializationException("Expected ascending/descending/ignore but found: " + x.toString)
      }
      
      val aliases: Seq[String] = obj.get("aliases") match {
        case Some(JsArray(arr)) =>
          arr.map(_.convertTo[String])
        case Some(x) =>
          throw new DeserializationException("Expected JsArray of JsStrings for aliases but found: " + x.toString)
        case None => Seq()
      }
      
      AvField(
        name = name, 
        doc = doc, 
        `type` = `type`, 
        default = default, 
        order = order, 
        aliases = aliases
      )
    }
    
    def readAvRecord(namespace: String, obj: Map[String, JsValue]): AvSchema = {
      val name = obj("name").convertTo[String]
      val nextNamespace = obj.get("namespace").map(_.convertTo[String]).getOrElse(namespace)
      val doc = obj.get("doc").map(_.convertTo[String])
      
      val aliases: Seq[String] = obj.get("aliases") match {
        case Some(JsArray(arr)) =>
          arr.map(_.convertTo[String])
        case Some(x) =>
          throw new DeserializationException("Expected JsArray of JsStrings for aliases but found: " + x.toString)
        case None => Seq()
      }
      
      val jsFields = obj("fields") match {
        case JsArray(arr) => arr
        case x => throw new DeserializationException("Expected JsArray of fields but found: " + x.toString)
      }
      
      val fields = jsFields.map { field =>
        readAvField(nextNamespace, field)
      }
      
      val meta = (obj - "type" - "name" - "namespace" - "doc" - "aliases" - "fields").toSeq
      
      AvRecord(
        name = name, 
        namespace = Some(nextNamespace), 
        doc = doc, 
        aliases = aliases, 
        fields = fields, 
        meta = meta
      )
    }
    
    def readAvMap(namespace: String, obj: Map[String, JsValue]): AvSchema = {
      val values = readEither(namespace, obj("values"))
      
      AvMap(values)
    }
    
    def readAvArray(namespace: String, obj: Map[String, JsValue]): AvSchema = {
      val items = readEither(namespace, obj("items"))
      
      AvArray(items)
    }
    
    def readAvEnum(namespace: String, obj: Map[String, JsValue]): AvSchema = {
      val name = obj("name").convertTo[String]
      val nextNamespace = obj.get("namespace").map(_.convertTo[String]).getOrElse(namespace)
      val doc = obj.get("doc").map(_.convertTo[String])
      
      val symbols: Seq[String] = obj("symbols") match {
        case JsArray(arr) =>
          arr.map(_.convertTo[String])
        case x =>
          throw new DeserializationException("Expected JsArray of JsStrings for symbols but found: " + x.toString)
      }
      
      AvEnum(
        name = name, 
        namespace = Some(nextNamespace), 
        doc = doc, 
        symbols = symbols
      )
    }
    
    def readAvFixed(namespace: String, obj: Map[String, JsValue]): AvSchema = {
      val name = obj("name").convertTo[String]
      val nextNamespace = obj.get("namespace").map(_.convertTo[String]).getOrElse(namespace)
      val size = obj("size").convertTo[Int]
      
      val aliases: Seq[String] = obj.get("aliases") match {
        case Some(JsArray(arr)) =>
          arr.map(_.convertTo[String])
        case Some(x) =>
          throw new DeserializationException("Expected JsArray of JsStrings for aliases but found: " + x.toString)
        case None => Seq()
      }
      
      AvFixed(
        name = name,
        namespace = Some(nextNamespace),
        aliases = aliases, 
        size = size
      )
    }
    
    def readAvUnion(namespace: String, types: Vector[JsValue]): AvSchema = {
      AvUnion(types.toArray.map(readEither(namespace, _)):_*)
    }
  }
}