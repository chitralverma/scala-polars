import sbt.Keys._
import sbt._

import Utils._
import Versions._

object ProjectDependencies {

  lazy val dependencies: Seq[Setting[_]] = Seq(
    libraryDependencies ++= {
      Seq(
        "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompat,
        "org.json4s" %% "json4s-native" % json4sVersion
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
  val scalaCollectionCompat = "2.9.0"
  val scalaParallelCollections = "1.0.4"
  val json4sVersion = "4.0.6"
}
