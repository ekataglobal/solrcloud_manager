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
          val matches: Seq[String] = getNodeListUsingIndicators(i, allowOfflineReferences)
          matches.size match {
            case 0 => throw new ManagerException(s"Could not determine a live node from '$indicator'")
            case 1 => acc ++ matches
            case _ => throw new ManagerException(s"Ambiguous node name '$indicator', possible matches: $matches")
          }
      }
    })
    if (nodeList.isEmpty)
      comment.warn("Couldn't find any nodes matching: " + indicators.mkString(","))
    nodeList
  }

  /**
    * Gets a known host name for a given string
    * @param indicator node indicator passed in by the user, this could be a host name or an IP, or
    *                      an exact representation of the node in ZK
    * @param allowOfflineReferences
    * @throw ManagerException if a host could not be safely determined
    * @return A known canonical host
    */
  def canonicalNodeName(indicator: String, allowOfflineReferences: Boolean = false): String = {
    val matches: Seq[String] = getNodeListUsingIndicators(indicator, allowOfflineReferences)
    matches.size match {
      case 0 => throw new ManagerException(s"Could not determine a live node from '$indicator'")
      case 1 => matches.head
      case _ => throw new ManagerException(s"Ambiguous node name '$indicator', possible matches: $matches")
    }
  }


  def getNodeListUsingRegEx(pattern: Regex, allowOfflineReferences: Boolean): Seq[String] = {
    val clusterStateNodeList: Set[String] = if (allowOfflineReferences) allNodes else liveNodes
    getNodeList(clusterStateNodeList,
                comparison = (s: String) => pattern.findFirstIn(s).nonEmpty,
                mapSuccessCriteria = (matches: Map[String,String]) => matches.size>0,
                setSuccessCriteria = (matches: Seq[String]) => matches.size>0
    )
  }


  /**
    *
    * @param indicator
    * @param allowOfflineReferences
    * @return returns Seq[String] instead of String to facilitate error handling
    */
  def getNodeListUsingIndicators(indicator:String, allowOfflineReferences: Boolean): Seq[String] = {
    val clusterStateNodeList: Set[String] = if (allowOfflineReferences) allNodes else liveNodes
    //If the value specified by the user exactly matches a node from the cluster state, return this value as is
    if (clusterStateNodeList.contains(indicator)) {
      Seq(indicator)
    } else {
      def exactMatchComparison = (s: String) => s == indicator
      def subStringMatchComparison = (s: String) => s.contains(indicator)
      def singleNodeMapSuccessCriteria: Map[String, String] => Boolean = (matches:Map[String,String]) => matches.size == 1
      def singleNodeSetSuccessCriteria: Seq[String] => Boolean = (matches: Seq[String]) => matches.size == 1

      //Attempt to resolve using exact match
      val exactMatches = getNodeList(clusterStateNodeList, exactMatchComparison, singleNodeMapSuccessCriteria, singleNodeSetSuccessCriteria)
      if(!exactMatches.isEmpty) { exactMatches } else {
        //Attempt to resolve using fuzzy match
        val subStringMatches = getNodeList(clusterStateNodeList, subStringMatchComparison, singleNodeMapSuccessCriteria, singleNodeSetSuccessCriteria)
        if(!subStringMatches.isEmpty) { subStringMatches } else {
          val ipPortMatches =  matchWithIPAndPort(indicator,clusterStateNodeList)
          if(!ipPortMatches.isEmpty) { ipPortMatches } else {
            Seq()
          }
        }
      }
    }
  }

  /**
    *  Useful for matching when there are multiple solr instances on the same node registered with
    *  cluster (registration will be with same node but different port)
    * @param indicator
    * @param clusterStateNodeList
    * @return
    */
  def matchWithIPAndPort(indicator: String, clusterStateNodeList: Set[String]): Seq[String] = {
    val chunks = indicator.split(':')
    val host = chunks.head
    val port = if (chunks.length > 1) ":" + chunks.last else ""
    val ipAndPort = InetAddress.getByName(host).getHostAddress + port
    val matches: Set[String] = clusterStateNodeList.filter((node) => node.contains(ipAndPort))
    matches.toSeq
  }

  /**
    *
    * @param clusterStateNodeList list of nodes from the cluster state
    * @param comparison
    * @param mapSuccessCriteria function to determine matching success when comparing against resolved node representations (IP or host)
    * @param setSuccessCriteria function to determine matching success when comparing against node representations from cluster state
    * @return
    */
  def getNodeList(clusterStateNodeList: Set[String], comparison: (String) => Boolean,
                  mapSuccessCriteria: Map[String,String] => Boolean,
                  setSuccessCriteria: Seq[String] => Boolean ): Seq[String] = {
    //First try by matching to the fully qualified domain name
    val dnsNodeMap: Map[String, String] = dnsNameMap(clusterStateNodeList)
    val dnsMatches = findNodes(dnsNodeMap, comparison, mapSuccessCriteria)
    if(!dnsMatches.isEmpty){ dnsMatches } else {
      //If matching via domain name didn't work, try matching to the IP address
      val ipNodeMap: Map[String, String] = ipNameMap(clusterStateNodeList)
      val ipMatches = findNodes(ipNodeMap, comparison, mapSuccessCriteria)
      if(!ipMatches.isEmpty){ ipMatches } else {
        //If either of these approaches do not work, trying matching with the list of node strings from the cluster state
        val clusterStateMatches = findNodes(clusterStateNodeList, comparison, setSuccessCriteria)
        if(!clusterStateMatches.isEmpty){ ipMatches } else {
          Seq()
        }
      }
    }
  }

  /**
    * Performs a comparison against a map which contains a resolved version of a node in the cluster (IP or host) as the key
    * and the string representation of this node in the cluster state
    * @param nodeComparisonMap map where key is the resolved name of a node(IP or Host) and value is its
    *                          string representation in the cluster state
    * @param comparison        function to use for comparison
    * @param successCriteria   function that takes the "matched" elements and determines success or failure e.g. match only 1, match atleast 1
    * @return
    */
  def findNodes(nodeComparisonMap: Map[String,String], comparison: (String) => Boolean, successCriteria: Map[String,String] => Boolean):Seq[String] = {
    val matchingMaps: Map[String, String] = nodeComparisonMap.filter{
      case (resolvedNodeString, clusterNodeString) => comparison(resolvedNodeString) || comparison(clusterNodeString)
    }
    if(successCriteria(matchingMaps)){
      matchingMaps.map({case (k,v) => v}).toSeq
    } else{
      Seq()
    }
  }

  /**
    * Performs the comparison directly against list of nodes from the cluster state
    * @param clusterStateNodeList
    * @param comparison
    * @param successCriteria
    * @return
    */
  def findNodes(clusterStateNodeList: Set[String], comparison: (String) => Boolean, successCriteria: Seq[String] => Boolean ): Seq[String] = {
    val clusterStateMatch: Seq[String] = clusterStateNodeList.filter{comparison(_)}.toSeq
    if(successCriteria(clusterStateMatch)){ clusterStateMatch } else {
      Seq()
    }
  }

}
