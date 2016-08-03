package com.whitepages.cloudmanager.clusterhealth
import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.log4j.Level

case class MissingLeaders() extends HealthCheck {
  override def check(manager: ClusterManager): Option[Seq[HealthIssue]] = {
    val state = manager.currentState
    val issues = for { collection <- state.collections
          replicas = state.replicasFor(collection)
          slices <- replicas.groupBy(_.sliceName) if !slices._2.exists(_.leader) } yield {
      HealthIssue(Level.ERROR, s"Missing leader for collection ${collection} slice ${slices._1}")
    }
    if (issues.isEmpty) None else Some(issues)
  }
}
