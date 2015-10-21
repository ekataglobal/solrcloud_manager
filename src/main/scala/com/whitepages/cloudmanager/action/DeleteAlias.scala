package com.whitepages.cloudmanager.action

import com.whitepages.cloudmanager.client.SolrRequestHelpers
import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.solr.common.params.CollectionParams.CollectionAction

case class DeleteAlias(aliasName: String) extends Action {
  override val preConditions: List[StateCondition] = List()

  override def execute(clusterManager: ClusterManager): Boolean = {
    // aliases aren't part of the cluster state object, so can't do this in postConditions
    if (clusterManager.aliasMap.contains(aliasName)) {
      val params = new ModifiableSolrParams
      params.set("action", CollectionAction.DELETEALIAS.toString)
      params.set("name", aliasName)

      val success = SolrRequestHelpers.submitRequest(clusterManager.client, params)
      // aliases aren't part of the cluster state object, so can't do this in postConditions
      success && !clusterManager.aliasMap.contains(aliasName)
    }
    else {
      comment.warn(s"Alias name not found: $aliasName")
      false
    }
  }

  override val postConditions: List[StateCondition] = List()

  override def toString = s"DeleteAlias: name: $aliasName"
}
