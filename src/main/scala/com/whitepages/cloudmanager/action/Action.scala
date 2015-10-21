package com.whitepages.cloudmanager.action

import com.whitepages.cloudmanager.ManagerSupport
import com.whitepages.cloudmanager.state.{ClusterManager, SolrState}

case class StateCondition(name: String, check: Conditions.Condition)

trait Action extends ManagerSupport {
  
  val name: String = this.getClass.getSimpleName

  val preConditions: List[StateCondition]
  def canPerform(state: SolrState) = checkAndReport(preConditions, state)

  def execute(clusterManager: ClusterManager): Boolean

  def perform(clusterManager: ClusterManager): Boolean = {
    if ( canPerform(clusterManager.currentState) ) {
      var actionSuccess = true

      if (execute(clusterManager)) {
        comment.info(s"Applied $this")
      }
      else {
        comment.info(s"Could not apply $this")
        actionSuccess = false
      }

      if (actionSuccess) {
        if (verify(clusterManager.currentState)) {
          comment.info(s"Verified $this")
        }
        else {
          comment.info(s"Failed to verify $this")
          actionSuccess = false
        }
      }

      actionSuccess
    }
    else false
  }

  val postConditions: List[StateCondition]
  def verify(state: SolrState) = checkAndReport(postConditions, state)

  private def check(conditions: List[StateCondition], state: SolrState): Boolean = conditions.forall(_.check(state))
  private def checkAndReport(conditions: List[StateCondition], state: SolrState): Boolean = {
    conditions.forall {
      condition => condition.check(state) match {
        case false => comment.warn(s"Check Failed: ${condition.name}"); false
        case true  => comment.debug(s"Check Succeeded: ${condition.name}"); true
      }
    }
  }

}
