import sbt.*

object Utils {

  lazy val nativeRoot = taskKey[File]("Directory pointing to the native project root.")

  def priorTo213(scalaVersion: String): Boolean =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, minor)) if minor < 13 => true
      case _ => false
    }

}
