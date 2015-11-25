package com.whitepages.cloudmanager.action

import com.whitepages.cloudmanager.client.{LukeStateResponse, ReplicationHandlerHelpers, SolrRequestHelpers}
import com.whitepages.cloudmanager.state.{SolrReplica, ClusterManager}
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.solr.client.solrj.impl.{HttpSolrClient, HttpSolrServer}
import org.apache.solr.client.solrj.{SolrClient, SolrServer}
import scala.util.{Success, Failure}
import scala.concurrent.duration._

/**
 * Copy raw index files from one node to another. Some limitations:
 *   - Intended ONLY for copying a piece of a given collection across two
 *     clusters with the SAME number of slices.
 *     If the collections don't have the same number of slices, the hash slicing could get assigned differently,
 *     and you'd end up with slices that don't have the same doc routing you expect.
 *   - The toCore MUST be empty, or it may decline to update because it thinks it's local version is newer.
 *   - Be sure to hard commit on the cluster you're copying from before you start.
 * @param fromCore The name of the core to copy, ie collection1_shard1_replica2
 * @param toCore The name of the core to copy into. This could be different to account for copying
 *               from one replica of a given shard into another
 * @param fromNode The node to copy from, ie http://10.8.100.42:8983/solr
 * @param confirm Whether to try to confirm the copy worked. (currently by comparing doc counts)
 */
case class FetchIndex(fromCore: String, toCore: String, fromNode: String, confirm: Boolean = true) extends Action {
  override val preConditions: List[StateCondition] = List(
    StateCondition("target has the named core", Conditions.coreNameExists(toCore))
  )

  override def execute(clusterManager: ClusterManager): Boolean = {
    val targetReplica = clusterManager.currentState.allReplicas.filter( (r) => r.core == toCore).head

    // don't use a CloudSolrClient for this stuff, go to the node directly
    val client = new HttpSolrClient(targetReplica.url)
    val fromClient = new HttpSolrClient(fromNode)

    val params = new ModifiableSolrParams
    params.set("command", "fetchindex")
    params.set("masterUrl", fromNode + "/" + fromCore)

    SolrRequestHelpers.submitRequest(client, params, s"/$toCore/replication") &&
      delay(3.seconds) &&
      ReplicationHandlerHelpers.waitForReplication(client, toCore) &&
      (if (confirm) insureDataCopy(fromClient, client) else true)
  }

  // TODO: Find a reliable method of insuring the data copied successfully
  // that works with non-static indexes
  private def insureDataCopy(fromClient: SolrClient, toClient: SolrClient): Boolean = {
    val detailsParams = new ModifiableSolrParams()
    detailsParams.set("show", "index")
    val fromResponse = SolrRequestHelpers.getSolrResponse(fromClient, detailsParams, s"/$fromCore/admin/luke")
    val toResponse = SolrRequestHelpers.getSolrResponse(toClient, detailsParams, s"/$toCore/admin/luke")
    (fromResponse, toResponse) match {
      case (Failure(e), _) => false
      case (_, Failure(e)) => false
      case (Success(fromRsp), Success(toRsp)) => {
        // running out of names here.
        val from = LukeStateResponse(fromRsp.rsp)
        val to = LukeStateResponse(toRsp.rsp)

        if (from.numDocs != to.numDocs) {
          comment.warn(s"Index doc count doesn't match, from: ${from.numDocs}, to: ${to.numDocs}")
          false
        }
        else true

      }
    }
  }

  override val postConditions: List[StateCondition] = List()

  override def toString =
    s"FetchIndex: from: ${SolrReplica.hostName(fromNode)}, fromCore: $fromCore}, toCore: $toCore"
}
