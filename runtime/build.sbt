import com.nitro.build._
import com.nitro.build.PublishHelpers._

// GAV coordinates
lazy val projectName = "avro-codegen-runtime"
name         := projectName
version      := semver.toString

// dependencies & resolvers
libraryDependencies ++= Seq(
  apacheAvro,
  "org.scalacheck" %% "scalacheck" % "1.13.1"
)
resolvers ++= Seq("Confluentic repository" at "http://packages.confluent.io/maven/")

// compile & runtime settings
scalaVersion := "2.11.7"
CompileScalaJava.librarySettings(devConfig)
javaOptions := jvmOpts

// publish settings
pubSettings
