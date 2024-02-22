import sbt.*
import sbt.Keys.*

object PublishingSettings {

  lazy val settings: Seq[Setting[_]] = Seq(
    publish / skip := false,
    publishArtifact := true,
    publishMavenStyle := true,
    externalResolvers += "GitHub Package Registry" at "https://maven.pkg.github.com/chitralverma/scala-polars",
    publishTo := Some(
      "GitHub Package Registry" at "https://maven.pkg.github.com/chitralverma/scala-polars"
    ),
    credentials += Credentials(
      realm = "GitHub Package Registry",
      host = "maven.pkg.github.com",
      userName = "chitralverma",
      passwd = sys.env.getOrElse("GITHUB_TOKEN", "")
    )
  )

}
