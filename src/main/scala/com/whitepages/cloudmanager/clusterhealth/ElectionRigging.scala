package com.whitepages.cloudmanager.clusterhealth
import java.util.regex.{Matcher, Pattern}

import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.log4j.Level
import org.apache.solr.common.cloud.ZkStateReader
import org.apache.zookeeper.KeeperException.NoNodeException

import scala.collection.JavaConverters._

// See SOLR-6498, and SOLR-8697
case class ElectionRigging() extends HealthCheck {

  // from org.apache.solr.cloud.LeaderElector
  private val NODE_NAME: Pattern = Pattern.compile(".*?/?(.*?-)(.*?)-n_\\d+")
  private def getNodeName(nStringSequence: String): String = {
    var result: String = null
    val m: Matcher = NODE_NAME.matcher(nStringSequence)
    if (m.matches) {
      result = m.group(2)
    }
    else {
      throw new IllegalStateException("Could not find regex match in:" + nStringSequence)
    }
    result
  }

  override def check(manager: ClusterManager): Option[Seq[HealthIssue]] = {
    val zkStateReader = manager.client.getZkStateReader
    val zkClient = zkStateReader.getZkClient
    val state = manager.currentState

    val issues = for { (collection, collectionReplicas) <- state.allReplicas.groupBy(_.collection)
                       (slice, sliceReplicas) <- collectionReplicas.groupBy(_.sliceName) } yield {
      val zkElectionEntries = try {
        zkClient.getChildren(ZkStateReader.getShardLeadersElectPath(collection, slice), null, false).asScala
      } catch {
        case ex: NoNodeException => List()
      }
      val electionNodeEntries = zkElectionEntries.map(getNodeName)
      val missingElectionNodes = sliceReplicas.filter(r => r.alive && electionNodeEntries.contains(r.node))
      val duplicateElectionNodes = electionNodeEntries.groupBy(identity).values.filter(_.size > 1)

      val missingIssues = missingElectionNodes.map(r => HealthIssue(Level.ERROR, "Missing from election: " + r.shortPrintFormat))
      val duplicateIssues = duplicateElectionNodes.map(dupe => {
        HealthIssue(Level.ERROR, s"Found ${dupe.length} election entries for Collection: $collection, Slice: $slice: Node: ${dupe.head}")
      })
      missingIssues ++ duplicateIssues
    }

    val allIssues = issues.flatten.toSeq
    if (allIssues.isEmpty) None else Some(allIssues)
  }
}
