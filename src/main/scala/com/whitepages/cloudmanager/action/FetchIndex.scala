package com.whitepages.cloudmanager.action

import com.whitepages.cloudmanager.state.{ReplicationStateResponse, ClusterManager}
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.solr.client.solrj.impl.HttpSolrServer
import org.apache.solr.client.solrj.SolrServer
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
 *   - I'm not sure what will happen if you try this copying from a cluster that's taking updates. At the least, the
 *     generation/version checks after the copy finishes will probably fail.
 * @param fromCore The name of the core to copy, ie collection1_shard1_replica2
 * @param toCore The name of the core to copy into. This could be different to account for copying
 *               from one replica of a given shard into another
 * @param fromNode The node to copy from, ie 10.8.100.42:8983
 */
case class FetchIndex(fromCore: String, toCore: String, fromNode: String, hostContext: String = "/solr") extends Action {
  override val preConditions: List[StateCondition] = List(
    StateCondition("target has the named core", Conditions.coreNameExists(toCore))
  )

  override def execute(clusterManager: ClusterManager): Boolean = {
    val targetReplica = clusterManager.currentState.allReplicas.filter( (r) => r.core == toCore).head
    val fromUrl = s"http://$fromNode/$hostContext"

    // don't use a CloudSolrServer for this stuff, go to the node directly
    val client = new HttpSolrServer(s"http://${targetReplica.node}/$hostContext")
    val fromClient = new HttpSolrServer(fromUrl)

    val params = new ModifiableSolrParams
    params.set("command", "fetchindex")
    params.set("masterUrl", fromUrl + "/" + fromCore)
    SolrRequestHelpers.submitRequest(client, params, s"/$toCore/replication") &&
      delay(5.seconds) &&
      ReplicationHelpers.waitForReplication(client, toCore) &&
      insureDataCopy(fromClient, client)
  }

  private def insureDataCopy(fromClient: SolrServer, toClient: SolrServer): Boolean = {
    val detailsParams = new ModifiableSolrParams()
    detailsParams.set("command", "details")
    val fromResponse = SolrRequestHelpers.getSolrResponse(fromClient, detailsParams, s"/$fromCore/replication")
    val toResponse = SolrRequestHelpers.getSolrResponse(toClient, detailsParams, s"/$toCore/replication")
    (fromResponse, toResponse) match {
      case (Failure(e), _) => false
      case (_, Failure(e)) => false
      case (Success(fromRsp), Success(toRsp)) => {
        // running out of names here.
        val from = ReplicationStateResponse(fromRsp.rsp)
        val to = ReplicationStateResponse(toRsp.rsp)

        if (from.generation != to.generation) {
          comment.warn(s"Index generation doesn't match, from: ${from.generation}, to: ${to.generation}")
          false
        }
        else if (from.indexVersion != to.indexVersion) {
          comment.warn(s"Index version doesn't match, from: ${from.indexVersion}, to: ${to.indexVersion}")
          false
        }
        else true

      }
    }
  }

  override val postConditions: List[StateCondition] = List()
}
