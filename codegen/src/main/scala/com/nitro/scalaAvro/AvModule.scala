package com.nitro.scalaAvro

import spray.json._
import PartialAvroJsonProtocol._

class AvModule(val toPartials: Map[AvReference, AvSchema]) {

  def keySet: Set[AvReference] =
    toPartials.keySet

  def get(key: AvReference): Option[org.apache.avro.Schema] =
    toPartials.get(key) map { avsc =>
      val (_, canonical) = makeCanonical(toPartials - key, avsc)
      canonical.toSchema
    }

  def apply(key: AvReference): org.apache.avro.Schema =
    get(key).get

  def get(name: String): Option[org.apache.avro.Schema] =
    get {
      JsString(name).convertTo[AvReference] match {
        case AvReference(None, refName) => AvReference(Some(""), refName)
        case x                          => x
      }
    }

  def apply(name: String): org.apache.avro.Schema =
    get(name).get

  /*
     * De-references only the first unseen reference. This is canonical avsc structure
     * per org.apache.avro Schema.Parser
     */
  private[this] def makeCanonical(
    unseen: Map[AvReference, AvSchema],
    av:     AvSchema
  ): (Map[AvReference, AvSchema], AvSchema) = {
      def dereferenceUnseen(
        unseen: Map[AvReference, AvSchema],
        either: Either[AvSchema, AvReference]
      ): (Map[AvReference, AvSchema], Either[AvSchema, AvReference]) =
        either match {
          case x @ Right(ref) => unseen.get(ref) match {
            case Some(avsc) =>
              val (nextUnseen, canonical) = makeCanonical(unseen - ref, avsc)
              (nextUnseen, Left(canonical))

            case None => (unseen, x)
          }

          case Left(avsc) =>
            val (nextUnseen, canonical) = makeCanonical(unseen, avsc)
            (nextUnseen, Left(canonical))
        }

    av match {
      case av: AvPrimitive => (unseen, av)

      case av: AvEnum      => (unseen, av)

      case av: AvFixed     => (unseen, av)

      case av: AvArray =>
        val (nextUnseen, items) = dereferenceUnseen(unseen, av.items)
        (nextUnseen, av.copy(items = items))

      case av: AvMap =>
        val (nextUnseen, values) = dereferenceUnseen(unseen, av.values)
        (nextUnseen, av.copy(values = values))

      case av: AvUnion =>
        val (nextUnseen, types) =
          av.types
            .foldLeft((unseen, Seq[Either[AvSchema, AvReference]]())) { (memo, next) =>
              val (unseen, seq) = memo
              val (nextUnseen, avsc) = dereferenceUnseen(unseen, next)
              (nextUnseen, seq ++ Seq(avsc))
            }
        (nextUnseen, AvUnion(types: _*))

      case av: AvRecord =>
        val (nextUnseen, fields) =
          av.fields
            .foldLeft((unseen, Seq[AvField]())) { (memo, field) =>
              val (unseen, seq) = memo
              val (nextUnseen, avsc) = dereferenceUnseen(unseen, field.`type`)
              val nextField = field.copy(`type` = avsc)
              (nextUnseen, seq ++ Seq(nextField))
            }
        (nextUnseen, av.copy(fields = fields))
    }
  }
}

object AvModule {

  def fromPartials(partials: Seq[AvSchema]): AvModule = {
    val (defs, refs) = partials.map(_.traverse).unzip
    val referables = defs.flatten
    val references = refs.flatten.distinct
    val lookups = referables.groupBy(_.reference).map {
      case (ref, schemas) =>
        val compact = schemas.map(_.copyWithReferencesOnly)
        if (compact.forall(_ == compact.head))
          ref -> compact.head
        else
          throw new Exception("Incompatible Schema restatement.")
    }
    val refsNotFound = references.toSet -- lookups.keySet
    if (refsNotFound.nonEmpty)
      throw new Exception("No Definition Found for Reference(s): "+refsNotFound)
    else
      new AvModule(lookups)
  }

  def fromJsonPartials(partials: Seq[JsValue]): AvModule =
    fromPartials {
      partials.map(_.convertTo[AvSchema])
    }

  def fromStringPartials(partials: Seq[String]): AvModule =
    fromJsonPartials {
      partials.map(_.parseJson)
    }
}