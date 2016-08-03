package com.whitepages.cloudmanager.clusterhealth

import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.log4j.Level

case class HealthIssue(level: Level, message: String)

trait HealthCheck {
  val name: String = this.getClass.getSimpleName
  def check(manager: ClusterManager): Option[Seq[HealthIssue]]
}
