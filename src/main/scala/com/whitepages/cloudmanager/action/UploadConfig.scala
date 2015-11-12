package com.whitepages.cloudmanager.action

import java.nio.file.{Files, Path}

import com.whitepages.cloudmanager.state.ClusterManager

import scala.util.Try

case class UploadConfig(path: Path, configName: String) extends Action {

  val requiredFiles = List("solrconfig.xml", "schema.xml")

  override def execute(clusterManager: ClusterManager): Boolean = {

    val attempt = Try(clusterManager.client.uploadConfig(path, configName))
    if (attempt.isFailure) {
      comment.error("Couldn't upload config", attempt.failed.get)
      false
    }
    else
      true

  }

  override val preConditions: List[StateCondition] =
    requiredFiles.map(f => StateCondition(s"$f exists in $path", (s) => Files.exists(path.resolve(f))))

  override val postConditions: List[StateCondition] = List(
    StateCondition(s"$configName exists", Conditions.configExists(configName))
  )

  override def toString = s"UploadConfig: dir: ${path.toAbsolutePath} configName: $configName"
}
