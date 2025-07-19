import sbt.*
import sbt.Keys.*

import Utils.*
import Versions.*

object ProjectDependencies {

  lazy val dependencies: Seq[Setting[_]] = Seq(
    libraryDependencies ++=
      Seq(
        "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompat,
        "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
        "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion
      ) ++
        (if (!priorTo213(scalaVersion.value))
           Seq(
             "org.scala-lang.modules" %% "scala-parallel-collections" % scalaParallelCollections
           )
         else Nil) ++
        (
          scalaVersion.value match {
            // Only include scala-reflect for Scala 2
            case v if v.startsWith("2.") => Seq("org.scala-lang" % "scala-reflect" % v)
            // No scala-reflect for Scala 3
            case _ => Seq.empty
          }
        )
  )

}

object Versions {
  val scalaCollectionCompat = "2.13.0"
  val scalaParallelCollections = "1.2.0"
  val jacksonVersion = "2.19.2"
}
