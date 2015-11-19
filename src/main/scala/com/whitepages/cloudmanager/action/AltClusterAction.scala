package com.whitepages.cloudmanager.action

import com.whitepages.cloudmanager.operation.Operation
import com.whitepages.cloudmanager.state.ClusterManager

object AltClusterAction {
  def apply(action: Action, boundManager: ClusterManager): AltClusterAction = AltClusterAction(Seq(action), boundManager)
}

/**
  * An action that encapsulates a set of actions against an alternative solrcloud cluster
  * @param actions Actions to perform on the other cluster
  * @param boundManager The ClusterManager for the other cluster
  */
case class AltClusterAction(actions: Seq[Action], boundManager: ClusterManager) extends Action {

  override def execute(clusterManager: ClusterManager): Boolean = Operation(actions).execute(boundManager)

  override val preConditions: List[StateCondition] = Nil
  override val postConditions: List[StateCondition] = Nil

  override def toString = {
    s"AltClusterAction: (${boundManager.client.getZkHost}) \n" + actions.map("\t\t" + _.toString).mkString("\n")
  }
}


