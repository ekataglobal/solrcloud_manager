package com.whitepages.cloudmanager.state

import org.apache.solr.common.cloud.{ZkStateReader, ClusterState, Replica, Slice}
import scala.util.Try
import scala.util.matching.Regex
import scala.collection.JavaConverters._
import java.net.InetAddress
import com.whitepages.cloudmanager.{ManagerSupport, ManagerException}

object SolrReplica {
  def hostName(nodeName: String) = {
    val chunks = nodeName.split(':')
    Try(InetAddress.getByName(chunks.head).getCanonicalHostName + ":" + chunks.tail.mkString("").replace('_','/'))
      .getOrElse(nodeName.replace('_', '/'))
  }
}
case class SolrReplica(collection: String, slice: Slice, replica: Replica, alive: Boolean) {
  import SolrReplica._
  lazy val leader = Option(slice.getLeader).getOrElse("").toString == replica.toString // TODO: Why aren't these the same object in some collections?
  lazy val activeSlice = slice.getState == Slice.State.ACTIVE
  lazy val activeReplica = replica.getState == Replica.State.ACTIVE && alive
  lazy val active = activeSlice && activeReplica
  lazy val core = replica.getStr(ZkStateReader.CORE_NAME_PROP)
  lazy val sliceName = slice.getName
  lazy val replicaName = replica.getName
  lazy val url = replica.getStr(ZkStateReader.BASE_URL_PROP)
  lazy val node = replica.getNodeName
  lazy val host = hostName(node)
}

case class SolrState(state: ClusterState, collectionInfo: CollectionInfo, configs: Set[String]) extends ManagerSupport {
  val printHeader = List(
    "Collection",
    "Slice",
    "ReplicaName",
    "Host",
    "Active",
    "CoreName"
  )
  def printReplicas() {
    comment.info("Nodes:")
    for { (node, nodeName) <- (allNodes.toList zip allNodes.toList.map(SolrReplica.hostName)).sortBy(_._2) } {
      comment.info(
        nodeName +
          " (" +
          (if (liveNodes.contains(node)) "up" else "down") +
          "/" +
          (if (unusedNodes.contains(node)) "unused" else "used") +
          ")"
      )
    }

    val infoTable: Seq[List[String]] = printHeader +: (for { replica <- allReplicas } yield {
      List(
        replica.collection,
        replica.sliceName.+(if (replica.leader) "*" else ""),
        replica.replicaName,
        replica.host,
        replica.active.toString,
        replica.core
      )
    })

    val colWidths = infoTable.transpose.map(s => s.maxBy(_.length).length)
    val lineFormat = colWidths.map(w => s"%-${w}s").mkString(" ")
    infoTable.foreach(row => comment.info(lineFormat.format(row:_*)))
  }

  lazy val collections = state.getCollections.asScala.toSeq.sorted
  private def extractSlicesMap(collection: String) = state.getCollection(collection).getSlicesMap.asScala
  private def extractReplicasMap(collection: String, slice: String) = extractSlicesMap(collection)(slice).getReplicasMap.asScala


  lazy val allReplicas = for {
    collection <- collections
    (sliceName, slice) <- extractSlicesMap(collection).toSeq.sortBy(_._1)
    (replicaName, node) <- extractReplicasMap(collection, sliceName).toSeq.sortBy(_._1)
  } yield SolrReplica(collection, slice, node, liveNodes.contains(node.getNodeName))

  lazy val liveNodes = state.getLiveNodes.asScala
  lazy val downNodes =  allReplicas.map(_.node).filterNot(liveNodes.contains)
  lazy val allNodes = liveNodes ++ downNodes
  lazy val unusedNodes = allNodes.filterNot(allReplicas.map(_.node).contains)
  lazy val activeReplicas = allReplicas.filter(_.active)
  lazy val inactiveReplicas = allReplicas.filterNot(activeReplicas.contains)

  def replicasFor(collection: String): Seq[SolrReplica] = allReplicas.filter(_.collection == collection)
  def replicasFor(collection: String, sliceName: String): Seq[SolrReplica] =
    replicasFor(collection).filter(_.slice.getName == sliceName)
  def liveReplicasFor(collection: String): Seq[SolrReplica] = replicasFor(collection).filter(_.active)
  def nodesWithCollection(collection: String) = replicasFor(collection).map(_.node).distinct
  def nodesWithoutCollection(collection: String) = liveNodes -- nodesWithCollection(collection)

  def canonicalNodeName(hostIndicator: String, allowOfflineReferences: Boolean = false): String = {
    val nodeList = if (allowOfflineReferences) allNodes else liveNodes
    if (nodeList.contains(hostIndicator)) {
      hostIndicator
    }
    else {
      val chunks = hostIndicator.split(':')
      val host = chunks.head
      val port = if (chunks.size > 1) ":" + chunks.last else ""
      val ipAndPort = InetAddress.getByName(host).getHostAddress + port
      val matches = nodeList.filter( (node) => node.contains(ipAndPort) )
      matches.size match {
        case 0 => throw new ManagerException(s"Could not determine a live node from '$hostIndicator'")
        case 1 => matches.head
        case _ => throw new ManagerException(s"Ambiguous node name '$hostIndicator', possible matches: $matches")
      }
    }

  }

}
