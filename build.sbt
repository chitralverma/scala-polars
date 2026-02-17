import DocSettings.*
import Utils.*

ThisBuild / publish / skip := true
ThisBuild / publishArtifact := false

/*
 ***********************
 * Core Module *
 ***********************
 */

lazy val core = project
  .in(file("core"))
  .withId("scala-polars")
  .settings(name := "scala-polars")
  .enablePlugins(GhpagesPlugin, SiteScaladocPlugin)
  .settings(
//    unidocSourceFilePatterns := Nil,
    git.remoteRepo := "git@github.com:chitralverma/scala-polars.git",
    SiteScaladoc / siteSubdirName := "api/latest"
  )
  .settings(ProjectDependencies.dependencies)
  .settings(GeneralSettings.commonSettings)
  .settings(
    publish / skip := false,
    publishArtifact := true,
    publishMavenStyle := true
  )
  .settings(
    nativeRoot := baseDirectory.value.toPath.resolveSibling("native").toFile,
    inConfig(Compile)(NativeBuildSettings.settings)
  )
  .settings(ExtraCommands.commands)
  .settings(ExtraCommands.commandAliases)
//  .configureUnidoc("scala-polars API Reference")

/*
 ***********************
 * Examples Module *
 ***********************
 */

lazy val examples = project
  .in(file("examples"))
  .withId("scala-polars-examples")
  .settings(name := "scala-polars-examples")
  .settings(GeneralSettings.commonSettings)
  .settings(
    Compile / packageBin / publishArtifact := false,
    Compile / packageDoc / publishArtifact := false,
    Compile / packageSrc / publishArtifact := false
  )
  .dependsOn(core)
