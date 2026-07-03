import sbt.*

import scala.sys.process.*

object Utils {

  lazy val nativeRoot =
    settingKey[File]("Directory pointing to the native project root.")

  def executeProcess(
      cmd: String,
      cwd: Option[File] = None,
      logger: Logger,
      infoOnly: Boolean = false,
      extraEnv: Seq[(String, String)] = Nil
  ): Unit = {
    val exitCode =
      Process(cmd, cwd, extraEnv *).run(getProcessLogger(logger, infoOnly)).exitValue()

    if (exitCode != 0) {
      sys.error(s"Failed to execute command `$cmd` with exit code $exitCode.")
    } else {
      logger.success(s"Successfully executed command `$cmd`.")
    }
  }

  def getProcessLogger(logger: Logger, infoOnly: Boolean = false): ProcessLogger =
    ProcessLogger(
      (o: String) => logger.info(o),
      (e: String) => if (infoOnly) logger.info(e) else logger.error(e)
    )

}
