package com.whitepages.cloudmanager.state

import com.whitepages.cloudmanager.client.{SystemRequestHelpers, SystemStateResponse, SolrCloudVersion, SolrRequestHelpers}
import org.apache.solr.common.cloud.{ZkStateReader, ZooKeeperException}
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.data.Stat
import scala.collection.JavaConverters._
import org.apache.solr.client.solrj.impl.{HttpSolrClient, CloudSolrClient}
import com.whitepages.cloudmanager.{ManagerException, ManagerSupport}

import scala.util.{Failure, Try, Random}

object ClusterManager extends ManagerSupport {

  private def zkFromNode(url: String) = {
    val zkHostOpt = SystemRequestHelpers.getSystemInfo(url).map(_.zkHost)
    if (zkHostOpt.isFailure)
      comment.error(s"Could not query system info from $url", zkHostOpt.failed.get)
    else if (zkHostOpt.get.isEmpty)
      comment.error(s"Could not get ZK string from $url")
    zkHostOpt.get.get  // This will throw an exception if either of the above errors were true
  }
  def apply(zk: String) = new ClusterManager(new CloudSolrClient(zk))
  def newFromNode(node: String) = apply(zkFromNode(node))
}
/**
 * Encapsulates the methods of getting and setting state in the cluster
 *
 * @param client A preconstructed CloudSolrServer client. This client will be connect()'ed if it wasn't already.
 */
case class ClusterManager(client: CloudSolrClient) extends ManagerSupport {

  try {
    client.connect()
  } catch {
    case e: ZooKeeperException => throw new ManagerException("Couldn't find solrcloud configuration in Zookeeper: " + e)
  }
  val stateReader = client.getZkStateReader

  def currentState = {
    stateReader.updateClusterState(true)
    SolrState(stateReader.getClusterState, CollectionInfo(configForCollection), configs)
  }
  def configForCollection(collection: String): String = stateReader.readConfigName(collection)

  def aliasMap: scala.collection.Map[String, String] = {
    stateReader.updateAliases()
    val aliases = stateReader.getAliases.getCollectionAliasMap
    if (aliases == null) Map() else aliases.asScala
  }
  def printAliases() {
    if (aliasMap.nonEmpty) {
      comment.info("Aliases:")
      aliasMap.foreach { case (alias, collection) => comment.info(s"\t$alias\t->\t$collection")}
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
  // Cache this, although certain things in this response (ie, heap usage) may change later.
  lazy val clusterSystemInfoResp: Try[SystemStateResponse] = {
    val randomReplica = Random.shuffle(currentState.activeReplicas).headOption
    randomReplica.map{ replica =>
      val client = new HttpSolrClient(replica.url)
      SystemRequestHelpers.getSystemInfo(client)
    }.getOrElse(Failure(new ManagerException("No active replicas to determine system info with")))
  }

  lazy val clusterVersion: SolrCloudVersion = {
    val version = clusterSystemInfoResp.map(_.solrVersion).getOrElse(SolrCloudVersion.unknown)

    // TODO: Change this to an Option[SolrCloudVersion]?
    // For now, let's assume if we're asking for it, we really do need it.
    if (version == SolrCloudVersion.unknown)
      comment.warn(s"Warning: Could not determine solr cluster version, version checks will test against $version")
    version
  }

  def printClusterVersion(): Unit = {
    comment.info(s"Cluster Version: $clusterVersion")
  }


  // I wish this was included in stateReader.getClusterState
  def configs: Set[String] = {
    client.getZkStateReader.getZkClient.getChildren(ZkStateReader.CONFIGS_ZKNODE, null, true).asScala.toSet
  }
  def configExists(configName: String) = configs.contains(configName)
  def printConfigs(): Unit = {
    comment.info("Config sets: " + configs.mkString(", "))
    comment.info("Config usage: ")
    val collections = currentState.collections
    collections.foreach(coll => {
      comment.info(s"\t$coll\t->\t " + configForCollection(coll) )
    })
    if (collections.isEmpty) comment.info("\t(no collections)")
  }



  def shutdown() = {
    stateReader.close()
    client.close()
  }

}

