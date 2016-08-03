package com.whitepages.cloudmanager.clusterhealth
import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.log4j.Level
import org.apache.solr.common.cloud.ZkStateReader
import org.apache.zookeeper.KeeperException.NoNodeException

import scala.collection.JavaConverters._

case class LeaderInitiatedRecovery() extends HealthCheck {

  // See org.apache.solr.cloud.ZkController
  private def lirPath(collection: String, slice: String): String =
    ZkStateReader.COLLECTIONS_ZKNODE + collection + "/leader_initiated_recovery/" + slice

  override def check(manager: ClusterManager): Option[Seq[HealthIssue]] = {
    val zkClient = manager.client.getZkStateReader.getZkClient
    val state = manager.currentState
    val issues = for { (collection, collectionReplicas) <- state.allReplicas.groupBy(_.collection)
          (slice, sliceReplicas) <- collectionReplicas.groupBy(_.sliceName) } yield {
      val lirCores = try {
        zkClient.getChildren(lirPath(collection, slice), null, false).asScala
      } catch {
        case ex: NoNodeException => List()
      }
      lirCores.map(c => HealthIssue(
        Level.WARN,
        "LIR for " + sliceReplicas.find(_.core == c).map(_.shortPrintFormat).getOrElse(s"(Internal error, couldn't find $c)")
      ))
    }
    val flatIssues = issues.toList.flatten
    if (flatIssues.isEmpty) None else Some(flatIssues)
  }
}
