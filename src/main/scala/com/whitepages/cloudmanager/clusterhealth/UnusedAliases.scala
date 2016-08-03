package com.whitepages.cloudmanager.clusterhealth
import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.log4j.Level

case class UnusedAliases() extends HealthCheck{
  override def check(manager: ClusterManager): Option[Seq[HealthIssue]] = {
    val state = manager.currentState
    val collections = state.collections.toSet
    val unused = manager.aliasMap.filterNot{ case (a, c) => collections.contains(c)}
    if (unused.isEmpty)
      None
    else {
      Some(unused.map{ case (a, c) => HealthIssue(Level.INFO, "Alias $a points to non-existent collection $c")}.toSeq)
    }
  }
}
