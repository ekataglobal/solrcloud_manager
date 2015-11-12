package com.whitepages.cloudmanager.action

import java.nio.file.{Path, Files}

import com.whitepages.cloudmanager.state.ClusterManager

import scala.util.Try

case class DownloadConfig(path: Path, configName: String) extends Action {

  val requiredFiles = List("solrconfig.xml", "schema.xml")

  override def execute(clusterManager: ClusterManager): Boolean = {

    val attempt = Try(clusterManager.client.downloadConfig(configName, path))
    if (attempt.isFailure) {
      comment.error("Couldn't download config", attempt.failed.get)
      false
    }
    else
      true

  }

  override val preConditions: List[StateCondition] = List(
    StateCondition(s"$configName exists", Conditions.configExists(configName)),
    StateCondition(s"$path exists locally", (s) => Files.exists(path))
  )

  override val postConditions: List[StateCondition] =
    requiredFiles.map(f => StateCondition(s"$f exists in $path", (s) => Files.exists(path.resolve(f))))

  override def toString = s"DownloadConfig: dir: ${path.toAbsolutePath} configName: $configName"
}
