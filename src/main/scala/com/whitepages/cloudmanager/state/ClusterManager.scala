package com.whitepages.cloudmanager.state

import com.whitepages.cloudmanager.client.{SystemStateResponse, SolrCloudVersion, SolrRequestHelpers}
import org.apache.solr.common.cloud.{ZooKeeperException}
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.data.Stat
import scala.collection.JavaConverters._
import org.apache.solr.client.solrj.impl.{HttpSolrClient, CloudSolrClient}
import com.whitepages.cloudmanager.{ManagerException, ManagerSupport}

import scala.util.Random

/**
 * Encapsulates the methods of getting and setting state in the cluster
 *
 * @param client A preconstructed CloudSolrServer client. This client will be connect()'ed if it wasn't already.
 */
case class ClusterManager(client: CloudSolrClient) extends ManagerSupport {
  def this(zk: String) = this(new CloudSolrClient(zk))

  try {
    client.connect()
  } catch {
    case e: ZooKeeperException => throw new ManagerException("Couldn't find solrcloud configuration in Zookeeper: " + e)
  }
  val stateReader = client.getZkStateReader

  def currentState = {
    stateReader.updateClusterState(true)
    SolrState(stateReader.getClusterState)
  }
  def aliasMap: scala.collection.Map[String, String] = {
    stateReader.updateAliases()
    val aliases = stateReader.getAliases.getCollectionAliasMap
    if (aliases == null) Map() else aliases.asScala
  }
  def printAliases() {
    if (aliasMap.nonEmpty) {
      comment.info("Aliases:")
      aliasMap.foreach { case (alias, collection) => comment.info(s"$alias\t->\t$collection")}
    }
  }

  def overseer(): String = {
    val zkClient = stateReader.getZkClient
    var data: Array[Byte] = null
    try {
      val payload = new String(zkClient.getData("/overseer_elect/leader", null, new Stat(), true))
      payload.replaceFirst(""".*"id":"[^-]*-""", "").replaceAll("""-.*""", "")
    }
    catch {
      case e: KeeperException.NoNodeException => {
        "none"
      }
    }
  }
  def printOverseer() {
    comment.info(s"Overseer: ${SolrReplica.hostName(overseer())}")
  }

  // Expensive, makes an http request to the cluster.
  lazy val clusterVersion: SolrCloudVersion = {
    val randomReplica = Random.shuffle(currentState.activeReplicas).head

    val client = new HttpSolrClient(randomReplica.url)
    val params = new ModifiableSolrParams
    val version = SolrRequestHelpers.getSolrResponse(client, params, "/admin/info/system")
      .map(r => SystemStateResponse(r.rsp).solrVersion).getOrElse(SolrCloudVersion.unknown)

    // TODO: Change this to an Option[SolrCloudVersion]?
    // For now, let's assume if we're asking for it, we really do need it.
    if (version == SolrCloudVersion.unknown)
      throw new ManagerException("Could not determine cluster version")
    version
  }

  def printClusterVersion(): Unit = {
    comment.info(s"Cluster Version: $clusterVersion")
  }


  def shutdown() = {
    stateReader.close()
    client.close()
  }

}

