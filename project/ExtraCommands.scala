import sbt.*
import sbt.Keys.*

import com.github.sbt.jni.plugins.JniJavah.autoImport.javah

object ExtraCommands {

  lazy val commandAliases: Seq[Setting[_]] = Seq(
    addCommandAlias("genHeaders", "javah")
  ).flatten

}
