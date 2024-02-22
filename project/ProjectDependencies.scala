import sbt.*
import sbt.Keys.*
import Utils.*
import Versions.*

object ProjectDependencies {

  lazy val dependencies: Seq[Setting[_]] = Seq(
    libraryDependencies ++= {
      Seq(
        "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompat,
        "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
      ) ++
        (if (!priorTo213(scalaVersion.value))
           Seq(
             "org.scala-lang.modules" %% "scala-parallel-collections" % scalaParallelCollections
           )
         else Nil)
    }
  )

}

object Versions {
  val scalaCollectionCompat = "2.11.0"
  val scalaParallelCollections = "1.0.4"
  val jacksonVersion = "2.16.1"
}
