ThisBuild / publish / skip := true
ThisBuild / publishArtifact := false

/*
 ***********************
 * Core Module *
 ***********************
 */

lazy val core = project
  .in(file("core"))
  .settings(ProjectDependencies.dependencies)
  .settings(GeneralSettings.settings)
  .settings(PublishingSettings.settings)
  .settings(
    inConfig(Compile)(NativeBuildSettings.settings)
  )
  .settings(ExtraCommands.commands)
  .settings(ExtraCommands.commandAliases)
