package com.whitepages.cloudmanager.action

import com.whitepages.cloudmanager.client.{SolrCloudVersion, SolrRequestHelpers}
import com.whitepages.cloudmanager.state.{SolrReplica, SolrState, ClusterManager}
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.solr.common.params.CollectionParams.CollectionAction

case class DeleteReplica(collection: String, slice: String, node: String, safetyFactor: Int = 1) extends Action {

  private val deleteReplicaCleanupFix = SolrCloudVersion(4,10)

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
    val replica = clusterManager.currentState.replicasFor(collection, slice).find(_.node == node)
    val replicaName = replica.map(_.replicaName)
    if (replicaName.isDefined) {
      if (clusterManager.clusterVersion < deleteReplicaCleanupFix)
        comment.warn("WARNING: DeleteReplica does NOT remove the files from disk until 4.10. See SOLR-6072.")
      val params = new ModifiableSolrParams
      params.set("action", CollectionAction.DELETEREPLICA.toString)
      params.set("collection", collection)
      params.set("shard", slice)
      params.set("replica", replicaName.get)

      val submitSuccess = SolrRequestHelpers.submitRequest(clusterManager.client, params)

      // If the replica we're deleting is down right now, and the node is too, the request for deletion may
      // return an error trying to forward the delete to the node hosting that replica.
      // In that case, we simply have to rely on the postCondition check below to validate the delete actually worked.
      submitSuccess || !replica.exists(_.active)
    }
    else {
      comment.warn("Couldn't figure out the replica name from the node")
      false
    }
  }

  override val postConditions: List[StateCondition] = List(
    StateCondition("replica no longer exists", Conditions.sliceIncludesNode(collection, slice, node).andThen(!_))
  )

  override def toString = s"DeleteReplica: collection: $collection, slice: $slice, node: ${SolrReplica.hostName(node)}, safetyFactor: $safetyFactor"
}
