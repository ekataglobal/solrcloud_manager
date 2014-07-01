package com.whitepages.cloudmanager.state

import org.apache.solr.common.cloud.{ZkStateReader, ClusterState, Replica, Slice}
import scala.util.matching.Regex
import scala.collection.JavaConverters._
import java.net.InetAddress
import com.whitepages.cloudmanager.ManagerException


case class SolrReplica(collection: String, slice: Slice, replica: Replica) {
  lazy val leader = slice.getLeader == replica
  lazy val activeSlice = slice.getState == Slice.ACTIVE
  lazy val activeReplica = replica.getStr(ZkStateReader.STATE_PROP) == ZkStateReader.ACTIVE
  lazy val active = activeSlice && activeReplica
  lazy val core = replica.getStr(ZkStateReader.CORE_NAME_PROP)
  lazy val sliceName = slice.getName
  lazy val replicaName = replica.getName
  lazy val url = replica.get("base_url")
  lazy val node = replica.getNodeName
}

case class SolrState(state: ClusterState) {
  val printHeader = List(
    "Collection",
    "Slice",
    "ReplicaName",
    "Host",
    "ActiveSlice",
    "ActiveReplica",
    "CoreName"
  ).mkString("\t")
  def printReplicas() {
    println(printHeader)
    for {replica <- allReplicas} {
      println(List(
        replica.collection,
        replica.sliceName.+(if (replica.leader) "*" else ""),
        replica.replicaName,
        hostName(replica.node),
        replica.activeSlice,
        replica.active,
        replica.core
      ).mkString("\t"))
    }
  }

  lazy val collections = state.getCollections.asScala.toSeq.sorted
  private def extractSlicesMap(collection: String) = state.getCollection(collection).getSlicesMap.asScala
  private def extractReplicasMap(collection: String, slice: String) = extractSlicesMap(collection)(slice).getReplicasMap.asScala


  lazy val allReplicas = for {
    collection <- collections
    (sliceName, slice) <- extractSlicesMap(collection).toSeq.sortBy(_._1)
    (replicaName, node) <- extractReplicasMap(collection, sliceName).toSeq.sortBy(_._1)
  } yield SolrReplica(collection, slice, node)

  lazy val liveNodes = state.getLiveNodes.asScala
  lazy val unusedNodes = liveNodes.filterNot(allReplicas.map(_.node).contains)
  lazy val activeReplicas = allReplicas.filter(_.active)
  lazy val inactiveReplicas = allReplicas.filterNot(activeReplicas.contains)

  def replicasFor(collection: String): Seq[SolrReplica] = allReplicas.filter(_.collection == collection)
  def replicasFor(collection: String, sliceName: String): Seq[SolrReplica] =
    replicasFor(collection).filter(_.slice.getName == sliceName)
  def nodesWithCollection(collection: String) = replicasFor(collection).map(_.node).distinct
  def nodesWithoutCollection(collection: String) = liveNodes -- nodesWithCollection(collection)


  private[this] final val coreNameConventionPat = new Regex("""shard(\d+)_replica(\d+)""", "shardNum", "replicaNum")
  private[this] final val coreNameConvention = """%s_shard%s_replica%s"""
  def nextCoreNameFor(collection: String, sliceName: String) = {
    val lastReplica = replicasFor(collection, sliceName).map(_.core).map { name =>
      coreNameConventionPat.findFirstIn(name) match {
        case Some(coreNameConventionPat(shardNum, replicaNum)) => (shardNum.toInt, replicaNum.toInt)
        case None => (0,0)
      }
    }.sorted.last

    coreNameConvention.format(collection, lastReplica._1, lastReplica._2 + 1)
  }


  def canonicalNodeName(hostIndicator: String): String = {
    if (liveNodes.contains(hostIndicator)) {
      hostIndicator
    }
    else {
      val chunks = hostIndicator.split(':')
      val host = chunks.head
      val port = if (chunks.size > 1) ":" + chunks.last else ""
      val ipAndPort = InetAddress.getByName(host).getHostAddress + port
      val matches = liveNodes.filter( (node) => node.contains(ipAndPort) )
      matches.size match {
        case 0 => throw new ManagerException(s"Could not determine a live node from '$hostIndicator'")
        case 1 => matches.head
        case _ => throw new ManagerException(s"Ambiguous node name '$hostIndicator', possible matches: $matches")
      }
    }

  }

  def hostName(nodeName: String) = {
    val chunks = nodeName.split(':')
    InetAddress.getByName(chunks.head).getCanonicalHostName + ":" + chunks.tail.mkString("").replace('_','/')
  }
}
