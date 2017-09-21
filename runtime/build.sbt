import com.nitro.build._
import com.nitro.build.PublishHelpers._

// GAV coordinates
lazy val projectName = "avro-codegen-runtime"
name         := projectName
version      := semver.toString

// dependencies & resolvers
libraryDependencies ++= Seq(
  apacheAvroDep,
  "org.scalacheck" %% "scalacheck" % "1.13.4"
)
resolvers ++= Seq("Confluentic repository" at "http://packages.confluent.io/maven/")

// compile & runtime settings
scalaVersion := scala212v
CompileScalaJava.librarySettings(devConfig)

// publish settings
pubSettings

scalacOptions := {
  val old = scalacOptions.value
  scalaVersion.value match {
    case sv if sv.startsWith("2.12") => old diff List("-Yinline-warnings", "-optimise")
    case _                           => old
  }
}
