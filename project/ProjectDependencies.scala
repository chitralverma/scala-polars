import sbt.*
import sbt.Keys.*

import com.github.sbt.jacoco.JacocoPlugin.autoImport.*
import com.github.sbt.jacoco.report.JacocoReportSettings
import com.github.sbt.jacoco.report.formats.*
import scoverage.ScoverageKeys.*
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

  /** Test deps for `core`. Versions pinned to the last JDK 8-compatible releases (CI tests on JDK
    * 8). ScalaTest + JUnit run under one `Test` pass.
    */
  lazy val testDependencies: Seq[Setting[_]] = Seq(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
      "com.github.sbt" % "junit-interface" % junitInterfaceVersion % Test,
      "junit" % "junit" % junitVersion % Test
    ),
    testFrameworks := Seq(TestFrameworks.ScalaTest, TestFrameworks.JUnit),
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v")
  )

  /** Scala coverage (sbt-scoverage). scoverage instruments Scala only; Java is covered by
    * [[jacocoSettings]]. The JNI `@native` declarations are FFI surface, excluded from coverage.
    */
  lazy val coverageSettings: Seq[Setting[_]] = Seq(
    coverageExcludedPackages := "com\\.github\\.chitralverma\\.polars\\.internal\\.jni\\..*",
    coverageFailOnMinimum := false
  )

  /** Java coverage (sbt-jacoco), for the lone Java source `JSeries.java` that scoverage cannot
    * instrument. Emits XML (Codecov) + HTML. JaCoCo also sees Scala classes — scoverage owns
    * those, so the JNI FFI declarations are excluded here too.
    */
  lazy val jacocoSettings: Seq[Setting[_]] = Seq(
    jacocoReportSettings := JacocoReportSettings()
      .withTitle("scala-polars Java coverage")
      .withFormats(JacocoReportFormats.XML, JacocoReportFormats.HTML),
    jacocoExcludes := Seq("com.github.chitralverma.polars.internal.jni.*")
  )

}

object Versions {
  val scalaCollectionCompat = "2.14.0"
  val scalaParallelCollections = "1.2.0"
  val jacksonVersion = "2.22.0"

  // Test stack — pinned to JDK 8-compatible releases (CI tests on JDK 8).
  val scalaTestVersion = "3.2.19"
  val junitVersion = "4.13.2"
  val junitInterfaceVersion = "0.13.3"
}
