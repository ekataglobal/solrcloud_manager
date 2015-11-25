package com.whitepages.cloudmanager.action

import com.whitepages.cloudmanager.client.SolrRequestHelpers
import com.whitepages.cloudmanager.state.{SolrReplica, ClusterManager}
import org.apache.solr.common.params.CollectionParams.CollectionAction
import org.apache.solr.common.params.ModifiableSolrParams


case class CreateCollection(collection: String,
                            numSlices: Int,
                            configName: String,
                            maxShardsPerNode: Option[Int] = None,
                            replicationFactor: Option[Int] = None,
                            createNodeSet: Option[Seq[String]] = None,
                            async: Boolean = false ) extends Action {


  override val preConditions: List[StateCondition] = List(
    StateCondition("collection doesn't already exist", Conditions.collectionExists(collection).andThen(!_))
  )

  override def execute(clusterManager: ClusterManager): Boolean = {
    val client = clusterManager.client

    if (clusterManager.configExists(configName)) {
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
    }
    else false

  }

  override val postConditions: List[StateCondition] = List(
    StateCondition("collection exists", Conditions.collectionExists(collection))
  )

  override def toString = {
    s"CreateCollection: name: $collection, slices: $numSlices, config: $configName" +
    s", maxShardsPerNode: ${maxShardsPerNode.getOrElse(1)}" +
    s", replicationFactor: ${replicationFactor.getOrElse(1)}" +
    s", nodes: " + createNodeSet.map(_.map(SolrReplica.hostName).mkString(",")).getOrElse("any") +
    s", async: $async"
  }
}
