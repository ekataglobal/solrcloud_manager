package com.whitepages.cloudmanager.action

import java.nio.file.Paths

import com.whitepages.cloudmanager.client.{ReplicationHandlerHelpers, SolrRequestHelpers}
import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.common.params.ModifiableSolrParams

import scala.util.{Success, Try}

case class RestoreIndex(core: String, backupDir: String, checkStatus: Boolean = true) extends Action {

  private def identicalIndexPaths(a: Try[Option[String]], b: Try[Option[String]]) = (a, b) match {
    case (Success(p1), Success(p2)) => p1 == p2
    case _ => false
  }
  private def getIndexPath(client: SolrClient) =
    ReplicationHandlerHelpers.getReplicationDetails(client, core).map(_.indexPath)

  override def execute(clusterManager: ClusterManager): Boolean = {
    // go to the node directly when using the replication handler
    val targetReplica = clusterManager.currentState.allReplicas.filter( (r) => r.core == core ).head
    val url = targetReplica.url
    val client = new HttpSolrClient(url)

    val params = new ModifiableSolrParams
    params.set("command", "restore")
    params.set("location", backupDir)

    SolrRequestHelpers.submitRequest(client, params, s"/$core/replication") &&
      (!checkStatus || ReplicationHandlerHelpers.waitForRestore(client, core))
  }

  override val preConditions = List(
    StateCondition("core exists", Conditions.coreNameExists(core))
  )
  override val postConditions: List[StateCondition] = Nil
}
