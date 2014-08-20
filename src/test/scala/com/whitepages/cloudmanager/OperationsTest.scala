package com.whitepages.cloudmanager

import org.junit.runner.RunWith
import org.junit.Assert._
import com.carrotsearch.randomizedtesting.RandomizedRunner
import org.apache.solr.SolrTestCaseJ4
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.solr.common.params.CollectionParams.CollectionAction
import org.apache.log4j.Level
import com.whitepages.cloudmanager.action._
import com.whitepages.cloudmanager.operation.{Operations, Operation}
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope
import com.whitepages.cloudmanager.operation.Operation
import com.whitepages.cloudmanager.action.CreateCollection
import scala.Some
import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.solr.common.cloud.ZkStateReader
import scala.collection.JavaConverters._


@SolrTestCaseJ4.SuppressSSL
@RunWith(classOf[RandomizedRunner])
@ThreadLeakScope(Scope.NONE)  // disable the usual lucene test case leak checking
class OperationsTest extends ManagerTestBase {
  override def managerTest() = {
    val clusterManager = new ClusterManager(cloudClient)

    // get a clean slate - cluster with no collections
    assertTrue(Operation(Seq(DeleteCollection(oldCollectionName))).execute(cloudClient))
    assertTrue(Operation(Seq(DeleteCollection("control_collection"))).execute(cloudClient))

    testWipeNode(clusterManager)
    testPopulateCluster(clusterManager)
    testFillCluster(clusterManager)
    testCleanCluster(clusterManager)

  }

  def testPopulateCluster(clusterManager: ClusterManager): Unit = {
    val numSlices = 2
    val targetNode = clusterManager.currentState.liveNodes.head
    val createCollection = Operation(Seq(CreateCollection("testcollection", numSlices, "conf1", Some(numSlices), None, Some(Seq(targetNode)))))
    assertTrue(createCollection.execute(cloudClient))

    // populate by putting all shards on each node
    assertTrue(Operations.populateCluster(clusterManager, "testcollection", numSlices).execute(cloudClient))
    assertEquals(nodeCount + 1, clusterManager.currentState.nodesWithCollection("testcollection").size)
    val replicasMinusTargetNode = clusterManager.currentState.replicasFor("testcollection").filterNot(_.node == targetNode)
    // since we are putting all the slices on each node, the number of replicas for each shard should be the same as the number of nodes
    assertTrue(replicasMinusTargetNode.groupBy(_.sliceName).values.forall(_.size == nodeCount))

    // reset
    assertTrue(Operation(Seq(DeleteCollection("testcollection"))).execute(cloudClient))
    assertTrue(createCollection.execute(cloudClient))

    // populate by putting one shard per node
    assertTrue(Operations.populateCluster(clusterManager, "testcollection", 1).execute(cloudClient))
    assertEquals(nodeCount + 1, clusterManager.currentState.nodesWithCollection("testcollection").size)
    val replicasMinusTargetNode2 = clusterManager.currentState.replicasFor("testcollection").filterNot(_.node == targetNode)
    // since we're putting one slice per node, the number of replicas for each shard should be the number of nodes divided by the number of shards
    assertTrue(replicasMinusTargetNode2.groupBy(_.sliceName).values.forall(_.size == nodeCount / numSlices))

    // cleanup
    assertTrue(Operation(Seq(DeleteCollection("testcollection"))).execute(cloudClient))
  }

  def testWipeNode(clusterManager: ClusterManager): Unit = {
    // a complete slice exists on each node, with a certain replicationFactor
    // a replicationFactor of >= 2 is required or the DeleteReplica safety factor will prevent removal
    val numSlices = 2
    val replicationFactor = 2
    val targetNodes = clusterManager.currentState.liveNodes.take(replicationFactor).toSeq
    val createCollection = Operation(Seq(CreateCollection("testwipe", numSlices, "conf1", Some(numSlices), Some(replicationFactor), Some(targetNodes))))
    assertTrue(createCollection.execute(cloudClient))

    assertTrue(Operations.wipeNode(clusterManager, targetNodes.head).execute(cloudClient))
    assertTrue("replicas were removed from target node", clusterManager.currentState.allReplicas.filter(_.node == targetNodes.head).isEmpty)

    // cleanup
    assertTrue(Operation(Seq(DeleteCollection("testwipe"))).execute(cloudClient))
  }

  def testFillCluster(clusterManager: ClusterManager): Unit = {
    val liveNodes = clusterManager.currentState.liveNodes.size
    def addCollection(collection: String, numSlices: Int, maxSlicesPerNode: Int, replicationFactor: Int) {
      assertEquals(0, numSlices % maxSlicesPerNode)
      val neededNodes = (numSlices / maxSlicesPerNode) * replicationFactor
      val targetNodes = clusterManager.currentState.liveNodes.take(neededNodes).toSeq
      val createCollection = Operation(Seq(CreateCollection(collection, numSlices, "conf1", Some(numSlices), Some(replicationFactor), Some(targetNodes))))
      assertTrue(createCollection.execute(cloudClient))
    }
    // test with two shards per node, with an initial replicationFactor of 2
    addCollection("testfill", 2, 2, 2)
    assertEquals(2, clusterManager.currentState.nodesWithCollection("testfill").size)

    assertTrue(Operations.fillCluster(clusterManager, "testfill").execute(cloudClient))
    assertEquals("should have expanded to all nodes", liveNodes, clusterManager.currentState.nodesWithCollection("testfill").size)
    val countBySlice = clusterManager.currentState.replicasFor("testfill").groupBy(_.sliceName).map{ case (slice, replicas) => (slice, replicas.size)}
    assertTrue("All shards should exist on all nodes", countBySlice.forall{ case (slice, count) => count == liveNodes})

    // reset
    assertTrue(Operation(Seq(DeleteCollection("testfill"))).execute(cloudClient))

    // test with two shards, one per node, with an initial replicationFactor of 1
    addCollection("testfill", 2, 1, 1)
    assertEquals(2, clusterManager.currentState.nodesWithCollection("testfill").size)

    assertTrue(Operations.fillCluster(clusterManager, "testfill").execute(cloudClient))
    assertEquals("should have expanded to all nodes", liveNodes, clusterManager.currentState.nodesWithCollection("testfill").size)

    val sliceCounts = clusterManager.currentState.replicasFor("testfill").groupBy(_.sliceName).map{ case (slice, replicas) => replicas.size }
    // five nodes, one slice per node, so...
    assertEquals("one slice should be on three nodes", 1, sliceCounts.count(_ == 3))
    assertEquals("the other slice should be on two nodes", 1, sliceCounts.count(_ == 2))

  }

  def testCleanCluster(clusterManager: ClusterManager): Unit = {
    val controlPort = controlJetty.getBaseUrl.getPort.toString  // the "control" jetty has a convienent handle, so we'll use that
    val controlNode = clusterManager.currentState.liveNodes.filter(_.contains(controlPort)).head
    val targetNodes = Seq(clusterManager.currentState.liveNodes.filterNot(_.contains(controlPort)).head, controlNode)
    // one shard, two replicas
    val createCollection = Operation(Seq(CreateCollection("testcollection", 1, "conf1", Some(1), Some(2), Some(targetNodes))))
    assertTrue(createCollection.execute(cloudClient))

    assertEquals("both nodes have the collection", 2, clusterManager.currentState.nodesWithCollection("testcollection").length)

    // shut down one of the nodes
    controlJetty.stop()
    Thread.sleep(2000)

    assertTrue(Operations.cleanCluster(clusterManager, "testcollection").execute(cloudClient))
    assertEquals("only one node has the collection", 1, clusterManager.currentState.nodesWithCollection("testcollection").length)

    controlJetty.start(true) // restore normalicy
  }


}
