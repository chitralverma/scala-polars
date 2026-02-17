import sbt.*
import sbt.Keys.*

import com.github.sbt.jni.plugins.JniJavah.autoImport.javah

object ExtraCommands {

  lazy val cleanHeaders =
    taskKey[Unit]("Removes all previously generated headers")

  lazy val commandAliases: Seq[Setting[_]] = Seq(
    addCommandAlias("genHeaders", ";cleanHeaders; javah")
  ).flatten

  lazy val commands: Seq[Setting[_]] = Seq(
    cleanHeaders := {
      import scala.reflect.io.Directory

      val headerDir = (javah / target).value
      val directory = new Directory(headerDir)

      directory.deleteRecursively()
      sLog.value.info(s"Removed headers directory $headerDir")
    }
  )

}
