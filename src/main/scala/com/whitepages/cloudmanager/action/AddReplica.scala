package com.whitepages.cloudmanager.action

import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.solr.common.params.CollectionParams.CollectionAction
import scala.util.Try
import com.whitepages.cloudmanager.state.ClusterManager
import scala.concurrent.duration._

case class AddReplica(collection: String, slice: String, copyTo: String, waitUntilActive: Boolean = true, waitTimeoutSec: Int = -1) extends Action {
  override val preConditions: List[StateCondition] = List(
    StateCondition("collection exists", Conditions.collectionExists(collection)),
    StateCondition("live replica exists", Conditions.liveReplicaCount(collection, slice).andThen(_ > 0)),
    StateCondition("target node exists", Conditions.nodeExists(copyTo)),
    StateCondition("target node doesn't have the slice yet", Conditions.sliceIncludesNode(collection, slice, copyTo).andThen(!_))
  )

  override def execute(clusterManager: ClusterManager): Boolean = {
    println(s"Issuing command for ${this.name}: Collection: $collection, Slice: $slice, copying onto: $copyTo")

    val params = new ModifiableSolrParams
    params.set("action", CollectionAction.ADDREPLICA.toString)
    params.set("collection", collection)
    params.set("node", copyTo)
    params.set("shard", slice)
    val success = SolrRequestHelpers.submitRequest(clusterManager.client, params)

    success && (!waitUntilActive || Conditions.waitForState(clusterManager, Conditions.activeSliceOnNode(collection, slice, copyTo), waitTimeoutSec))
  }

  override val postConditions: List[StateCondition] = List(
    StateCondition("target node has the slice", Conditions.sliceIncludesNode(collection, slice, copyTo))
  )
}
