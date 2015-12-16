package com.nitro.example

import com.nitro.example.messages._
import shapeless._

object Main extends App {

  val p1 = Point(name = Some("p1"), x = 1, y = 2)
  val p2 = Point(name = Some("p2"), x = 3, y = 4)
  val p3 = Point(name = Some("p3"), x = 7, y = 8)

  val line = Line(name = Some("line"), start = p1, end = p2)

  val polyLine = PolyLine(name = None, Vector(p1, p2, p3))

  val circle = Circle(name = Some("c1"), center = p1, radius = 15)

  //todo: enum for type to show off feature
  val meta = Metadata(timestamp = 123L, `type` = Some(RequestType.Foo), headers = Map("foo" -> "bar", "baz" -> "foo"))

  type Geometry = Point :+: Line :+: PolyLine :+: Circle :+: CNil

  val geometry = Vector(
    Coproduct[Geometry](p1),
    Coproduct[Geometry](line),
    Coproduct[Geometry](polyLine),
    Coproduct[Geometry](circle)
  )

  val user = "the_kraken"

  val req = DrawRequest(user, meta, geometry)

}
