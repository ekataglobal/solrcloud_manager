package com.whitepages.cloudmanager.action

import com.whitepages.cloudmanager.state.{ClusterManager, SolrState}
import scala.annotation.tailrec

object Conditions {
  def collectionExists(collection: String) = (state: SolrState) => state.collections.contains(collection)
  def activeCollection(collection: String) = (state: SolrState) => state.replicasFor(collection).forall(_.active)
  def liveReplicaCount(collection: String, slice: String) = (state: SolrState) => state.replicasFor(collection, slice).count(_.active)
  def nodeExists(node: String) = (state: SolrState) => state.liveNodes.contains(node)
  def replicaExists(collection: String, slice: String, replica: String) = (state: SolrState) => state.replicasFor(collection, slice).find(_.replicaName == replica).nonEmpty
  def coreNameExists(name: String) = (state: SolrState) => state.allReplicas.map(_.core).contains(name)
  def sliceIncludesNode(collection: String, slice: String, node: String) =
    (state: SolrState) => state.replicasFor(collection, slice).filter(_.node == node).nonEmpty
  def activeSliceOnNode(collection: String, slice: String, node: String) =
    (state: SolrState) => state.replicasFor(collection, slice).find(_.node == node) match {
      case Some(replica) => replica.active
      case None => false
    }

  @tailrec
  def waitForState(stateFactory: ClusterManager, condition: (SolrState) => Boolean, timeoutSec: Int = 300): Boolean = {
    condition(stateFactory.currentState) match {
      case true => true
      case false if timeoutSec == 0 => false
      case false if (timeoutSec > 0 || timeoutSec < 0) => {
        if (timeoutSec % 10 == 0) println("Waiting for condition")
        Thread.sleep(1000)
        // if a -1 timeout was initially used, this could int-overflow in about 70 years. Oh well.
        waitForState(stateFactory, condition, timeoutSec - 1)
      }
    }
  }


}
