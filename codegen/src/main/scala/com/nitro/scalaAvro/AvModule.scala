package com.nitro.scalaAvro
import spray.json._
import org.apache.avro.Schema
import PartialAvroJsonProtocol._

/**
 * @author ebiggs
 */
class AvModule(private val lookups: Map[AvReference, AvSchema]) {
    def keySet = lookups.keySet
    
    def lookup(key: AvReference): Option[org.apache.avro.Schema] = lookups.get(key) map { avsc =>
      makeCanonical(lookups - key, avsc).toSchema
    }
    
    /*
     * De-references only the first unseen reference. This is canonical avsc structure
     * per org.apache.avro Schema.Parser
     */
    private[this] def makeCanonical(unseen: Map[AvReference, AvSchema], av: AvSchema): AvSchema = {
      def dereferenceUnseen(unseen: Map[AvReference, AvSchema], either: Either[AvSchema, AvReference]): (Map[AvReference, AvSchema], Either[AvSchema, AvReference]) = either match {
        case x @ Right(ref) => unseen.get(ref) match {
          case Some(avsc) => 
            val nextUnseen = unseen - ref
            (nextUnseen, Left(makeCanonical(nextUnseen, avsc)))
          case None => (unseen, x)
        }

        case x => (unseen, x)
      }
      
      av match {
        case av: AvPrimitive => av
        case av: AvEnum => av
        case av: AvFixed => av
        case av: AvArray =>
          val (_, items) = dereferenceUnseen(unseen, av.items)
          av.copy(items = items)
        case av: AvMap => 
          val (_, values) = dereferenceUnseen(unseen, av.values)
          av.copy(values = values)
        case av: AvUnion =>
          val (_, types) = av.types.foldLeft((unseen, Seq[Either[AvSchema, AvReference]]())) { (memo, next) =>
            val (unseen, seq) = memo
            val (nextUnseen, avsc) = dereferenceUnseen(unseen, next)
            (nextUnseen, seq ++ Seq(avsc))
          }
          av.copy(types = types)
        case av: AvRecord => 
          val (_, fields) = av.fields.foldLeft((unseen, Seq[AvField]())) { (memo, field) =>
            val (unseen, seq) = memo
            val (nextUnseen, avsc) = dereferenceUnseen(unseen, field.`type`)
            val nextField = field.copy(`type` = avsc)
            (nextUnseen, seq ++ Seq(nextField))
          }
          av.copy(fields = fields)
      }
    }
  }
  
  object AvModule {
    def fromPartials(partials: Seq[AvSchema]) = {
      val (defs, refs) = partials.map(_.traverse).unzip
      val referables = defs.flatten
      val references = refs.flatten.distinct
      val lookups = referables.groupBy(_.reference).map { case (ref, schemas) =>
        val compact = schemas.map(_.copyWithReferencesOnly)
        compact.forall(x => x == compact.head) match {
          case true => ref -> compact.head
          case false => throw new Exception("Incompatible Schema restatement.")
        }
      }
      val refsNotFound = references.toSet -- lookups.keySet
      if (!refsNotFound.isEmpty) {
        throw new Exception("No Definition Found for Reference(s): " + refsNotFound)
      } else {
        new AvModule(lookups)
      }
    }
    
    def fromJsonPartials(partials: Seq[JsValue]) = fromPartials {
      partials.map(_.convertTo[AvSchema])
    }
    
    def fromStringPartials(partials: Seq[String]) = fromJsonPartials {
      partials.map(_.toJson)
    }
  }