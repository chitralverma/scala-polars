import Utils.*

val enableCoverage =
  sys.env.get("RUN_COVERAGE").contains("true") || sys.props.get("jacoco.enable").contains("true")

/*
 ***********************
 * Root (aggregate) *
 ***********************
 */

// Aggregating root to prevent publishing of the default root project.
lazy val root = (project in file("."))
  .withId("scala-polars-root")
  .settings(name := "scala-polars-root")
  .settings(GeneralSettings.commonSettings)
  .disablePlugins(JacocoPlugin)
  .settings(
    publish / skip := true,
    publishArtifact := false
  )
  .aggregate(core, examples)

/*
 ***********************
 * Core Module *
 ***********************
 */

lazy val core = project
  .in(file("core"))
  .withId("scala-polars")
  .settings(name := "scala-polars")
  .configure { p =>
    if (enableCoverage) p.enablePlugins(JacocoPlugin)
    else p.disablePlugins(JacocoPlugin)
  }
  .settings(ProjectDependencies.dependencies)
  .settings(ProjectDependencies.testDependencies)
  .settings(ProjectDependencies.coverageSettings)
  .settings(ProjectDependencies.jacocoSettings)
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
  .configure { p =>
    if (enableCoverage) p.enablePlugins(JacocoPlugin)
    else p.disablePlugins(JacocoPlugin)
  }
  .settings(
    publish / skip := true,
    publishArtifact := false
  )
  .dependsOn(core)

/*
 * ***********************
 * Unused Key Linting Exclusion *
 * ***********************
 */

Global / excludeLintKeys ++= Set(
  git.gitUncommittedChanges,
  git.gitDescribedVersion,
  publishArtifact
)
