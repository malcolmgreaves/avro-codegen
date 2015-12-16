package com.nitro.scalaAvro

import java.nio.file.Files
import com.nitro.example.messages._
import foobar._

import com.nitro.scalaAvro.runtime.{GeneratedMessageCompanion, _}
import org.apache.avro.file._
import org.apache.avro.generic._
import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks

trait SerializeDeserializeSpec[T <: GeneratedMessage with Message[T]]
  extends PropSpec
  with GeneratorDrivenPropertyChecks
  with Matchers {

  def companion: GeneratedMessageCompanion[T]

  property(s"serialize and deserialize ${companion.getClass.getName} messages") {
      forAll(companion._arbitrary){ (msg: Message[T]) => try {
        val tmp = Files.createTempFile("sandbox_", ".avro").toFile
        val schema = companion.schema

        //write single message to disk
        //println(s"attempting to serialize: $msg")
        val userDatumWriter = new GenericDatumWriter[GenericRecord](schema)
        val dataFileWriter = new DataFileWriter[GenericRecord](userDatumWriter)
        dataFileWriter.create(schema, tmp)
        dataFileWriter.append(msg.toMutable)
        dataFileWriter.close()

        // Deserialize Users from disk
        val userDatumReader: GenericDatumReader[GenericRecord] =
          new GenericDatumReader[GenericRecord](schema)
        val dataFileReader: DataFileReader[GenericRecord] =
          new DataFileReader[GenericRecord](tmp, userDatumReader)

        dataFileReader.hasNext shouldEqual true
        val out = companion.fromMutable(dataFileReader.next)
        dataFileReader.close()

        out shouldEqual msg
      } catch {
          case t: Throwable =>
            println(t.getMessage)
            t.printStackTrace()
            throw t
        }
    }
  }
}


class DrawRequestSpec extends SerializeDeserializeSpec[DrawRequest]{
  override def companion = DrawRequest
}

class OptionSpec extends SerializeDeserializeSpec[OptionTest]{
  override def companion = OptionTest
}

class ArraySpec extends SerializeDeserializeSpec[ArrayTest]{
  override def companion = ArrayTest
}

class MapSpec extends SerializeDeserializeSpec[MapTest]{
  override def companion = MapTest
}

class EnumSpec extends SerializeDeserializeSpec[EnumTest]{
  override def companion = EnumTest
}

class RectangleSpec extends SerializeDeserializeSpec[Rectangle]{
  override def companion = Rectangle
}

class FooSpec extends SerializeDeserializeSpec[Foo]{
  override def companion = Foo
}
