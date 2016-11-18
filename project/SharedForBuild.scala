import sbt._
import Keys._

object SharedForBuild extends Build {

  // use sbt-dev-settings to configure

  publishArtifact in ThisBuild := false

  import com.nitro.build._
  import PublishHelpers._

  lazy val semver = SemanticVersion(0, 3, 5, isSnapshot = false)

  lazy val apacheAvroDep = "org.apache.avro" % "avro" % "1.8.1"

  private[this] def githubUrl(id: String) = 
    new URL("http", "github.com", s"/$id")

  lazy val pluginDevelopers = Seq(
    Developer("pkinsky",        "Paul Kinsky",     "pkinsky@gmail.com",         githubUrl("pkinsky")),
    Developer("malcolmgreaves", "Malcolm Greaves", "greaves.malcolm@gmail.com", githubUrl("malcolmgreaves")),
    Developer("ebiggs",         "Eric Biggs",      "ebiggs@gmail.com",          new URL("http", "ebiggs.com", ""))
  )

  lazy val scala211v = "2.11.8"
  lazy val scala210v = "2.10.6"

  // ** NOTE **    We want to upgrade to Java 8 ASAP. Spark is still stuck at Java 7.
  // [JIRA Issue]  https://issues.apache.org/jira/browse/SPARK-6152

  lazy val devConfig =  {
    import CompileScalaJava._
    Config.spark.copy(scala = 
      ScalaConfig(
        fatalWarnings = false,
        logImplicits = false,
        optimize = true,
        crossCompile = Seq(scala211v, scala210v),
        inlineWarn = true
      )
    )
  }

  lazy val jvmOpts = JvmRuntime.settings(devConfig.jvmVer)

  lazy val pubSettings =
    Publish.settings(
      Repository.github("Nitro", "avro-codegen"),
      pluginDevelopers,
      ArtifactInfo.sonatype(semver),
      License.apache20
    )

}
