package com.whitepages.cloudmanager.state

import org.apache.solr.common.cloud.ZkStateReader
import scala.collection.JavaConverters._
import org.apache.solr.client.solrj.impl.CloudSolrServer

/**
 * Encapsulates the methods of getting and setting state in the cluster
 *
 * @param client A preconstructed CloudSolrServer client. This client will be connect()'ed if it wasn't already.
 */
case class ClusterManager(client: CloudSolrServer) {
  def this(zk: String) = this(new CloudSolrServer(zk))
  
  client.connect()
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
      println("Aliases:")
      aliasMap.foreach { case (alias, collection) => println(s"$alias\t->\t$collection")}
    }
  }

  def shutdown() = {
    stateReader.close()
    client.shutdown()
  }
}

