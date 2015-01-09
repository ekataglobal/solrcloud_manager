package com.whitepages.cloudmanager.action

import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.common.params.CollectionParams.CollectionAction
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.solr.common.cloud.ZkStateReader
import scala.collection.JavaConverters._
import com.whitepages.cloudmanager.ManagerSupport

object CreateCollection extends ManagerSupport {
  /**
   * Tests whether a given collection configName exists in ZK.
   * Can't put this check in Conditions, since the necessary data isn't in the SolrState
   * @param clusterManager
   * @param configName
   * @return Boolean whether the named config exists
   */
  def configExists(clusterManager: ClusterManager, configName: String) = {
    val client = clusterManager.client

    // helps work around SOLR-5638 - the collections API doesn't do this check until it's already half-done,
    // which would leave the new collection in a bad state that requires manually deleting core dirs from the nodes
    // before you can try again
    if (!client.getZkStateReader.getZkClient.exists(ZkStateReader.CONFIGS_ZKNODE + "/" + configName, true)) {
      val knownConfigs = client.getZkStateReader.getZkClient.getChildren(ZkStateReader.CONFIGS_ZKNODE, null, true).asScala
      comment.warn(s"The specified config '$configName' couldn't be found in ZK. Known configs are: ${knownConfigs.mkString(", ")}")
      false
    }
    else true
  }
}

case class CreateCollection(collection: String,
                            numSlices: Int,
                            configName: String,
                            maxShardsPerNode: Option[Int] = None,
                            replicationFactor: Option[Int] = None,
                            createNodeSet: Option[Seq[String]] = None,
                            async: Boolean = false ) extends Action {
  import CreateCollection._

  override val preConditions: List[StateCondition] = List(
    StateCondition("collection doesn't already exist", Conditions.collectionExists(collection).andThen(!_))
  )

  override def execute(clusterManager: ClusterManager): Boolean = {
    val client = clusterManager.client

    if (configExists(clusterManager, configName)) {
      val params = new ModifiableSolrParams
      params.set("action", CollectionAction.CREATE.toString)
      params.set("name", collection)
      params.set("numShards", numSlices)
      params.set("collection.configName", configName)
      maxShardsPerNode.map(params.set("maxShardsPerNode", _))
      replicationFactor.map(params.set("replicationFactor", _))
      createNodeSet.map((ns) => params.set("createNodeSet", ns.mkString(",")))

      val submitResult =
        if (async)
          SolrRequestHelpers.submitAsyncRequest(client, params) && Conditions.waitForState(clusterManager, Conditions.activeCollection(collection))
        else
          SolrRequestHelpers.submitRequest(client, params)
      submitResult && Conditions.waitForState(clusterManager, Conditions.activeCollection(collection))
    } else false

  }

  override val postConditions: List[StateCondition] = List(
    StateCondition("collection exists", Conditions.collectionExists(collection))
  )
}
