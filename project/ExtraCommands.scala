import sbt.*
import sbt.Keys.*
import Utils.*
import com.github.sbt.jni.plugins.JniJavah.autoImport.javah

object ExtraCommands {

  lazy val cleanHeaders =
    taskKey[Unit]("Removes all previously generated headers")
  lazy val cargoFmt =
    taskKey[Unit]("Formats native module and its Cargo.toml.")
  lazy val cargoCheck =
    taskKey[Unit]("Checks the formatting of native module and its Cargo.toml.")

  lazy val commandAliases: Seq[Setting[_]] = Seq(
    addCommandAlias("cleanAll", ";cleanHeaders; clean; cleanFiles; reload"),
    addCommandAlias("genHeaders", ";cleanHeaders; javah"),
    addCommandAlias("fmtAll", ";scalafmtAll; scalafmtSbt; javafmtAll; cargoFmt; reload"),
    addCommandAlias(
      "fmtCheckAll",
      ";scalafmtCheckAll; scalafmtSbtCheck; javafmtCheckAll; cargoCheck"
    )
  ).flatten

  lazy val commands: Seq[Setting[_]] = Seq(
    cleanHeaders := {
      import scala.reflect.io.Directory

      val headerDir = (javah / target).value
      val directory = new Directory(headerDir)

      directory.deleteRecursively()
      sLog.value.info(s"Removed headers directory $headerDir")
    },
    cargoFmt := {
      val nativeRootDir = nativeRoot.value: @sbtUnchecked
      val cmds = Seq(
        "cargo fix --allow-dirty --allow-staged",
        "cargo sort",
        "cargo fmt --verbose --all"
      )

      executeProcesses(cmds, cwd = Some(nativeRootDir), sLog.value, infoOnly = true)
    },
    cargoCheck := {
      val nativeRootDir = nativeRoot.value: @sbtUnchecked
      val cmds = Seq(
        "cargo fmt --check --all",
        "cargo sort --check"
      )

      executeProcesses(cmds, cwd = Some(nativeRootDir), sLog.value, infoOnly = true)
    }
  )

}
