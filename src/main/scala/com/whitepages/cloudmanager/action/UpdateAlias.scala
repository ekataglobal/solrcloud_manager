package com.whitepages.cloudmanager.action

import com.whitepages.cloudmanager.client.SolrRequestHelpers
import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.solr.common.params.CollectionParams.CollectionAction

case class UpdateAlias(aliasName: String, aliasTo: Seq[String]) extends Action {
  override val preConditions: List[StateCondition] = List(
    StateCondition("Target collection names exist",
      (state) => aliasTo.forall(Conditions.collectionExists(_)(state))
    )
  )

  override def execute(clusterManager: ClusterManager): Boolean = {
    val params = new ModifiableSolrParams
    params.set("action", CollectionAction.CREATEALIAS.toString)
    params.set("name", aliasName)
    params.set("collections", aliasTo.mkString(","))
    val success = SolrRequestHelpers.submitRequest(clusterManager.client, params)

    // aliases aren't part of the cluster state object, so can't do this in postConditions
    val aliases = clusterManager.aliasMap
    success &&
      aliases.contains(aliasName) &&
      aliases(aliasName).split(",").sorted.mkString(",") == aliasTo.sorted.mkString(",")
  }

  override val postConditions: List[StateCondition] = List()
}
