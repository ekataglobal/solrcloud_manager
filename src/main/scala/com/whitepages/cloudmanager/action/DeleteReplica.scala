package com.whitepages.cloudmanager.action

import com.whitepages.cloudmanager.state.{SolrState, ClusterManager}
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.solr.common.params.CollectionParams.CollectionAction

case class DeleteReplica(collection: String, slice: String, node: String, safetyFactor: Int = 1) extends Action {
  override val preConditions: List[StateCondition] = List(
    StateCondition("replica exists on node", Conditions.sliceIncludesNode(collection, slice, node)),
    StateCondition(s"enough replicas to keep $safetyFactor alive", (state) => {
      val safetyTest = if (Conditions.activeSliceOnNode(collection, slice, node)(state)) {
        // we're deleting an active node, so make sure we have active spares
        (activeNodeCount: Int) => activeNodeCount > safetyFactor
      }
      else {
        // we're deleting an inactive node, so this operation doesn't change the active node count,
        // but we still don't want to risk deleting the last replica
        (activeNodeCount: Int) => activeNodeCount >= 1
      }
      Conditions.liveReplicaCount(collection, slice).andThen(safetyTest)(state)
    })
  )

  override def execute(clusterManager: ClusterManager): Boolean = {
    val replicaName = clusterManager.currentState.replicasFor(collection, slice).find(_.node == node).map(_.replicaName)
    if (replicaName.isDefined) {
      val params = new ModifiableSolrParams
      params.set("action", CollectionAction.DELETEREPLICA.toString)
      params.set("collection", collection)
      params.set("shard", slice)
      params.set("replica", replicaName.get)

      SolrRequestHelpers.submitRequest(clusterManager.client, params)
    }
    else {
      comment.warn("Couldn't figure out the replica name from the node")
      false
    }
  }

  override val postConditions: List[StateCondition] = List(
    StateCondition("replica no longer exists", Conditions.sliceIncludesNode(collection, slice, node).andThen(!_))
  )
}
