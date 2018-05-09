package com.whitepages.cloudmanager.state

import java.net.InetAddress

import com.whitepages.cloudmanager.{ManagerException, ManagerSupport}
import org.apache.solr.common.cloud.{ClusterState, Replica, Slice, ZkStateReader}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.matching.Regex

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
  lazy val stateName = if (slice.getState != Slice.State.ACTIVE) slice.getState.toString else replica.getState.toString
  lazy val active = activeSlice && activeReplica
  lazy val core = replica.getStr(ZkStateReader.CORE_NAME_PROP)
  lazy val sliceName = slice.getName
  lazy val replicaName = replica.getName
  lazy val url = replica.getStr(ZkStateReader.BASE_URL_PROP)
  lazy val node = replica.getNodeName
  lazy val host = hostName(node)
  def shortPrintFormat = s"Collection: $collection Slice: $sliceName Replica: $replicaName, Host: $host, Core: $core"
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

  lazy val liveNodes = state.getLiveNodes.asScala.toSet
  lazy val downNodes =  allReplicas.map(_.node).filterNot(liveNodes.contains).toSet
  lazy val allNodes = liveNodes ++ downNodes
  lazy val unusedNodes = allNodes.filterNot(allReplicas.map(_.node).contains)
  lazy val activeReplicas = allReplicas.filter(_.active)
  lazy val inactiveReplicas = allReplicas.filterNot(activeReplicas.contains)


  def replicasFor(collection: String): Seq[SolrReplica] = allReplicas.filter(_.collection == collection)

  def replicasFor(collection: String, sliceName: String): Seq[SolrReplica] =
    replicasFor(collection).filter(_.slice.getName == sliceName)

  def liveReplicasFor(collection: String): Seq[SolrReplica] = replicasFor(collection).filter(_.active)
  def nodesWithCollection(collection: String): Seq[String] = replicasFor(collection).map(_.node).distinct
  def nodesWithoutCollection(collection: String): Set[String] = liveNodes -- nodesWithCollection(collection)

  /**
    *
    * This method take the representation of each node, resolves it and creates a map of
    *  fully qualified domain name -> node ZK representation
    *
    * @param nodeList list of ZK represenation of each node in the cluster
    * @return Map (dns name -> node zk representation)
    */
  def dnsNameMap(nodeList: Set[String] = liveNodes): Map[String,String] = {
    nodeList.map( node => InetAddress.getByName(node.take(node.indexOf(':'))).getCanonicalHostName -> node ).toMap
  }

  /**
    * This method take the representation of each node, resolves it and creates a map of
    * *  IP address -> node ZK representation
    *
    * @param nodeList list of ZK represenation of each node in the cluster
    * @return Map (ip address -> node zk representation)
    */
  def ipNameMap(nodeList: Set[String] = liveNodes): Map[String,String] = {
    nodeList.map( node => InetAddress.getByName(node.take(node.indexOf(':'))).getHostAddress -> node ).toMap
  }

  /**
    * Takes the value specified for nodes by the user via the CLI and returns their
    * corresponding string representation from the ZK cluster state
    *
    * @param indicators user passed in argument for nodes via the command line e.g. "all","empty", a comma separated
    *                   list of IPs or a regular expression
    * @param allowOfflineReferences
    * @param ignoreUnrecognized
    * @return
    */
  def mapToNodes(indicators: Option[Seq[String]], allowOfflineReferences: Boolean = false, ignoreUnrecognized: Boolean = false): Option[Seq[String]] = {
    indicators.map(mapToNodes(_, allowOfflineReferences, ignoreUnrecognized))
  }

  /**
    *
    * @param indicators user passed in argument for nodes via the command line e.g. "all","empty", a comma separated
    *                   list of IPs or a regular expression
    * @param allowOfflineReferences allow down nodes to be considered
    * @param ignoreUnrecognized
    * @return
    */
  def mapToNodes(indicators: Seq[String], allowOfflineReferences: Boolean, ignoreUnrecognized: Boolean): Seq[String] = {
    val nodeList: Seq[String] = indicators.foldLeft(Seq[String]())((acc, indicator) => {
      indicator.toLowerCase match {
        case "all"  =>
          val nodeList: Set[String] = if (allowOfflineReferences) allNodes else liveNodes
          acc ++ nodeList
        case "empty" =>
          val nodeList = if (allowOfflineReferences) unusedNodes else unusedNodes -- downNodes
          acc ++ nodeList.toSeq
        case r if r.startsWith("regex=") =>
          //If the user specified a regular expression
          val pattern: Regex = r.stripPrefix("regex=").r
          getNodeListUsingRegEx(pattern, allowOfflineReferences)
        case i =>
          //If a comma separated list of nodes is specified, then for each node
          val nodeName = Try(Seq(canonicalNodeName(i, allowOfflineReferences))).recover({
            case e: ManagerException if ignoreUnrecognized =>
              comment.warn(e.getMessage)
              Seq[String]()
          }).get
          acc ++ nodeName
      }
    })
    if (nodeList.isEmpty)
      comment.warn("Couldn't find any nodes matching: " + indicators.mkString(","))
    nodeList
  }


  /**
    * Gets a known host name for a given string
    * @param hostIndicator node indicator passed in by the user, this could be a host name or an IP, or
    *                      an exact representation of the node in ZK
    * @param allowOfflineReferences
    * @throw ManagerException if a host could not be safely determined
    * @return A known canonical host
    */
  def canonicalNodeName(hostIndicator: String, allowOfflineReferences: Boolean = false): String = {
    val rawNodeList = if (allowOfflineReferences) allNodes else liveNodes

    //If the value specified by the user exactly matches a node from the cluster state, return this value as is
    if (rawNodeList.contains(hostIndicator)) {
      hostIndicator
    }
    else {
      unambiguousFragment(hostIndicator, dnsNameMap(rawNodeList)).getOrElse(
        unambiguousFragment(hostIndicator, ipNameMap(rawNodeList)).getOrElse(
          {
            //Here we attempt to transform the indicator(user passed in value) instead of the values from the cluster state
            val chunks = hostIndicator.split(':')
            val host = chunks.head
            val port = if (chunks.length > 1) ":" + chunks.last else ""
            val ipAndPort = InetAddress.getByName(host).getHostAddress + port
            val matches = rawNodeList.filter((node) => node.contains(ipAndPort))
            matches.size match {
              case 0 => throw new ManagerException(s"Could not determine a live node from '$hostIndicator'")
              case 1 => matches.head
              case _ => throw new ManagerException(s"Ambiguous node name '$hostIndicator', possible matches: $matches")
            }
          }
        )
      )
    }
  }


  /**
    *
    * @param fragment          user passed in string for node identification
    * @param nodeComparisonMap map where key is the resolved name of a node(IP or Host) and value is its
    *                          string representation in the cluster state
    * @return
    */
  def unambiguousFragment(fragment: String, nodeComparisonMap: Map[String,String]): Option[String] = {
    findUnambigousNode(nodeComparisonMap, (s: String) => s == fragment)
      .orElse(findUnambigousNode(nodeComparisonMap, (s: String) => s.contains(fragment)))
  }

  /**
    *
    * @param nodeComparisonMap map where key is the resolved name of a node(IP or Host) and value is its
    *                          string representation in the cluster state
    * @param comparison        function to use for comparison
    * @return
    */
  def findUnambigousNode(nodeComparisonMap: Map[String,String], comparison: (String) => Boolean): Option[String] = {
    val matchingMaps = nodeComparisonMap.filter{
      case (resolvedNodeString, clusterNodeString) => comparison(resolvedNodeString) || comparison(clusterNodeString)
    }
    matchingMaps.toList match {
      case (resolvedNodeName, clusterNodeString) :: Nil => Some(clusterNodeString)
      case _ => None
    }
  }


  /**
    * This method takes the passed in regular expression and searches for nodes that match this pattern
    * In the first pass, the pattern attempts to match against fully qualified domain names
    * If no nodes matched in the first pass, it then attempts to match against the IP addresses
    * If no nodes matched in the second pass, it then attempts to match against the raw node representation returned by ZK
    * Return an empty sequence if none of these work
    *
    * @param pattern the pattern to use for matching nodes
    * @param allowOfflineReferences
    * @return
    */
  def getNodeListUsingRegEx(pattern: Regex, allowOfflineReferences: Boolean): Seq[String] = {
    val clusterStateNodeList = if (allowOfflineReferences) allNodes else liveNodes

    //First try by matching to the fully qualified domain name
    val dnsNodeList: Map[String, String] = dnsNameMap(clusterStateNodeList)
    val dnsMatch = dnsNodeList.filter{ case (k, v) => pattern.findFirstIn(k).nonEmpty}.values.toSeq
    if(!dnsMatch.isEmpty){ dnsMatch } else{
      //If matching via domain name didn't work, try matching to the IP addresses
      val ipNodeList: Map[String, String] = ipNameMap(clusterStateNodeList)
      val ipMatch = ipNodeList.filter{ case (k, v) => pattern.findFirstIn(k).nonEmpty}.values.toSeq
      if(!ipMatch.isEmpty){ ipMatch } else {
        //If either of these approaches do not work, trying matching with the unresolved list of nodes i.e. use the
        //node list from the cluster state without transforming it
        val clusterStateMatch = clusterStateNodeList.filter{pattern.findFirstIn(_).nonEmpty}.toSeq
        if(!clusterStateMatch.isEmpty){ clusterStateMatch } else {
          //return an empty sequence if nothing matches
          Seq()
        }
      }
    }
  }
}
