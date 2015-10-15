package com.whitepages.cloudmanager.action

import com.whitepages.cloudmanager.state.{ClusterManager, SolrState}
import scala.annotation.tailrec
import com.whitepages.cloudmanager.ManagerSupport
import scala.concurrent.duration._

object Conditions extends ManagerSupport {
  type Condition = (SolrState) => Boolean

  def collectionExists(collection: String): Condition = (state: SolrState) => state.collections.contains(collection)
  def activeCollection(collection: String): Condition = (state: SolrState) => state.replicasFor(collection).forall(_.active)
  def nodeExists(node: String): Condition = (state: SolrState) => state.liveNodes.contains(node)
  def replicaExists(collection: String, slice: String, replica: String): Condition =
    (state: SolrState) => state.replicasFor(collection, slice).exists(_.replicaName == replica)
  def coreNameExists(name: String): Condition = (state: SolrState) => state.allReplicas.map(_.core).contains(name)
  def sliceIncludesNode(collection: String, slice: String, node: String): Condition =
    (state: SolrState) => state.replicasFor(collection, slice).exists(_.node == node)
  def activeSliceOnNode(collection: String, slice: String, node: String): Condition =
    (state: SolrState) => state.replicasFor(collection, slice).find(_.node == node) match {
      case Some(replica) => replica.active
      case None => false
    }

  def liveReplicaCount(collection: String, slice: String) = (state: SolrState) => state.replicasFor(collection, slice).count(_.active)

  /**
   * Doesn't return until the condition is satisfied, or the timeout is reached
   * @param stateFactory
   * @param condition
   * @param timeoutSec In seconds, default 300. Use -1 for no timeout.
   * @return
   */
  @tailrec
  def waitForState(stateFactory: ClusterManager, condition: Condition, timeoutSec: Int = 300): Boolean = {
    condition(stateFactory.currentState) match {
      case true => true
      case false if timeoutSec == 0 => false
      case false if (timeoutSec > 0 || timeoutSec < 0) => {
        if (timeoutSec % 10 == 0) comment.debug("Waiting for condition")
        delay(1.second)
        // if a -1 timeout was initially used, this could int-overflow in about 70 years. Oh well.
        waitForState(stateFactory, condition, timeoutSec - 1)
      }
    }
  }


}
