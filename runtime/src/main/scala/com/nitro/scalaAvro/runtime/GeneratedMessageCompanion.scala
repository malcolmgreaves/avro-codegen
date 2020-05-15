package com.nitro.scalaAvro.runtime

import org.apache.avro.generic.GenericRecord
import org.scalacheck.Gen
import org.apache.avro.Schema

trait GeneratedEnum {
  def id: Int
  def name: String
  override def toString = name
}

trait GeneratedEnumCompanion[A <: GeneratedEnum] {
  type ValueType = A
  def fromValue(id: Int): A
  def fromMutable(generic: org.apache.avro.generic.GenericEnumSymbol[org.apache.avro.generic.GenericData.EnumSymbol]): A
  def values: Seq[A]
  def _arbitrary: Gen[A]
}

trait GeneratedMessage {
  def companion: GeneratedMessageCompanion[_]
}

trait Message[A] {
  def toMutable: GenericRecord
}

trait FromGenericRecord[T] {
  def fromMutable(x: GenericRecord): T
}

trait GeneratedMessageCompanion[A <: GeneratedMessage with Message[A]] extends FromGenericRecord[A] {
  def fromMutable(generic: org.apache.avro.generic.GenericRecord): A
  def toMutable(msg: A): org.apache.avro.generic.GenericRecord = msg.toMutable
  def _arbitrary: Gen[A]
  def schema: Schema

  protected def convertString(s: Any): String =
    s match {
      case s: String                    => s
      case u: org.apache.avro.util.Utf8 => new String(u.getBytes, "UTF-8")
    }
}

object AvroGenUtils {
  //avro expects utf-8 strings
  lazy val genAvroString = org.scalacheck.Gen.identifier.map(s => new String(s.getBytes("UTF-8"), "UTF-8"))
  def genAvroMap[V](gen: org.scalacheck.Gen[V]): Gen[Map[String, V]] = for {
    k <- genAvroString
    v <- gen
    m <- org.scalacheck.Gen.oneOf[Map[String, V]](Map.empty[String, V], genAvroMap(gen))
  } yield m.updated(k, v)

  def genAvroArray[T](gen: org.scalacheck.Gen[T]): Gen[Vector[T]] = for {
    v <- gen
    m <- org.scalacheck.Gen.oneOf[Vector[T]](Vector.empty[T], genAvroArray(gen))
  } yield m ++ Vector(v)
}
