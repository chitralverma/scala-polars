/*
 ***********************
 * Constants *
 ***********************
 */

val scala212 = "2.12.15"
val scala213 = "2.13.10"
val scala32 = "3.2.1"

val defaultScalaVersion = scala213
val allScalaVersions = Seq(scala212, scala213, scala32)

ThisBuild / scalaVersion := defaultScalaVersion
ThisBuild / turbo := true

lazy val commonSettings = Seq(
  organization := "org.polars",
  licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      id = "chitralverma",
      name = "Chitral Verma",
      email = "chitral.verma@gmail.com",
      url = url("https://github.com/chitralverma")
    )
  ),
  scalaVersion := defaultScalaVersion,
  crossScalaVersions := allScalaVersions,
  fork := true,
  scalacOptions ++= Seq("-target:17", "-Ywarn-unused:imports"),
  Compile / javacOptions ++= Seq("-source", "17"),
  Compile / compile / javacOptions ++= Seq(
    "-target",
    "17",
    "-Xlint:all",
    "-Xlint:-options",
    "-Werror"
  )
)

/*
 ***********************
 * Root Module *
 ***********************
 */

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "scala-polars-parent",
    crossScalaVersions := Nil, // Setting crossScalaVersions to Nil on the root project
    publish / skip := true
  )
  .aggregate(core, native)

/*
 ***********************
 * Native Module *
 ***********************
 */

lazy val native = project
  .in(file("native"))
  .settings(commonSettings)
  .settings(
    name := "scala-polars-native",
    nativeCompile / sourceDirectory := baseDirectory.value
  )
  .enablePlugins(JniNative)

/*
 ***********************
 * Core Module *
 ***********************
 */

lazy val coreLibraryDependencies = Seq()

lazy val core = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "scala-polars",
    Compile / mainClass := Some("org.polars.scala.polars.Main"),
    libraryDependencies ++= coreLibraryDependencies,
    sbtJniCoreScope := Compile, // because we use `NativeLoader`, not the `@nativeLoader` macro
    classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat // because of `turbo := true`, otherwise `java.lang.UnsatisfiedLinkError: com.github.sideeffffect.scalarustinterop.Divider.divideBy(I)I`
  )
  .dependsOn(native % Runtime)
