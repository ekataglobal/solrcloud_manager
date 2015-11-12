package com.whitepages.cloudmanager.action

import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.solr.common.cloud.ZkStateReader
import scala.collection.JavaConverters._

case class DeleteConfig(configName: String) extends Action {
  override def execute(clusterManager: ClusterManager): Boolean = {
    try {
      val client = clusterManager.client.getZkStateReader.getZkClient
      val configPath = ZkStateReader.CONFIGS_ZKNODE + "/" + configName
      val children = client.getChildren(configPath, null, false).asScala
      children.foreach(child => client.delete(configPath + "/" + child, -1, false))
      client.delete(configPath, -1, false)
      true
    }
    catch {
      case e: Exception =>
        comment.error("Couldn't delete", e)
        false
    }
  }

  override val preConditions: List[StateCondition] = List(
    StateCondition(s"$configName exists", Conditions.configExists(configName)),
    StateCondition(
      s"config $configName is not in use",
      (s) => !s.collections.exists(coll => s.collectionInfo.configName(coll) == configName)
    )
  )
  override val postConditions: List[StateCondition] = List(
    StateCondition(s"$configName no longer exists", Conditions.configExists(configName).andThen(!_))
  )

  override def toString = s"DeleteConfig: configName: $configName"
}
