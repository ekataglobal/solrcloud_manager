package com.whitepages.cloudmanager.clusterhealth
import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.log4j.Level

case class UnusedConfigsets() extends HealthCheck {
  override def check(manager: ClusterManager): Option[Seq[HealthIssue]] = {
    val state = manager.currentState
    val configs = manager.configs
    val usedConfigSets = state.collections.map(manager.configForCollection).toSet
    val unusedConfigSets = configs -- usedConfigSets
    if (unusedConfigSets.isEmpty)
      None
    else {
      Some(unusedConfigSets.map(c => HealthIssue(Level.INFO, s"Unused collection: $c")).toSeq)
    }
  }
}
