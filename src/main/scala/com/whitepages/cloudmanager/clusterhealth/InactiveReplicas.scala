package com.whitepages.cloudmanager.clusterhealth

import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.log4j.Level

case class InactiveReplicas() extends HealthCheck {
  override def check(manager: ClusterManager): Option[Seq[HealthIssue]] = {
    val state = manager.currentState
    val inactiveReplicas = state.inactiveReplicas
    if (inactiveReplicas.isEmpty)
      None
    else {
      Some(inactiveReplicas.map(r => HealthIssue(
        Level.WARN,
        s"Inactive state ${r.stateName} for ${r.shortPrintFormat}"
      )))
    }
  }
}
