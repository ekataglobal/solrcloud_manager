package com.whitepages.cloudmanager.action

import com.whitepages.cloudmanager.client.SolrRequestHelpers
import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.solr.common.params.CollectionParams.CollectionAction
import scala.collection.JavaConverters._

case class DeleteCollection(collection: String) extends Action {
  override val preConditions: List[StateCondition] = List(
    StateCondition("collection exists", Conditions.collectionExists(collection))
  )

  override def execute(clusterManager: ClusterManager): Boolean = {

    // aliases aren't part of the SolrState object, so can't do this in preConditions
    val currentAliases = clusterManager.aliasMap.filter{ case (_, value) => value == collection }.map(_._1)

    if (currentAliases.nonEmpty) {
      comment.warn("Refusing to delete a collection that an alias is currently pointing to. Delete or move the alias first.")
      comment.warn(s"Active aliases for $collection: ${currentAliases.mkString(", ")}")
      false
    } else {
      val params = new ModifiableSolrParams
      params.set("action", CollectionAction.DELETE.toString)
      params.set("name", collection)
      SolrRequestHelpers.submitRequest(clusterManager.client, params) &&
        Conditions.waitForState(clusterManager, Conditions.collectionExists(collection).andThen(!_))
    }
  }

  override val postConditions: List[StateCondition] = List(
    StateCondition("collection doesn't exist", Conditions.collectionExists(collection).andThen(!_))
  )

  override def toString = s"DeleteCollection: name: $collection"
}
