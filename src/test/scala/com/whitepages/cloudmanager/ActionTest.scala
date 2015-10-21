package com.whitepages.cloudmanager

import org.apache.lucene.util.LuceneTestCase
import org.junit.runner.RunWith
import org.junit.Assert._
import com.carrotsearch.randomizedtesting.RandomizedRunner
import org.apache.solr.SolrTestCaseJ4
import org.apache.log4j.Level
import com.whitepages.cloudmanager.action._
import com.whitepages.cloudmanager.operation.Operations
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope
import com.whitepages.cloudmanager.operation.Operation
import com.whitepages.cloudmanager.action.AddReplica
import com.whitepages.cloudmanager.action.UpdateAlias
import com.whitepages.cloudmanager.action.CreateCollection
import scala.Some
import com.whitepages.cloudmanager.state.ClusterManager
import com.whitepages.cloudmanager.action.DeleteReplica
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.client.solrj.{SolrQuery, SolrRequest}
import org.apache.solr.client.solrj.request.{QueryRequest, UpdateRequest}
import scala.collection.JavaConverters._
import scala.util.Random
import org.apache.solr.common.params.ModifiableSolrParams

@SolrTestCaseJ4.SuppressSSL
@RunWith(classOf[RandomizedRunner])
@ThreadLeakScope(Scope.NONE)  // disable the usual lucene test case leak checking
class ActionTest extends ManagerTestBase {

  val newCollectionName = "collection2"

  override def managerTest() = {
    val clusterManager = new ClusterManager(cloudClient)

    createCollection(newCollectionName, 2, 1, 1)
    blockUntilStable(clusterManager)

    testAddRemoveReplica(clusterManager)
    testAlias(clusterManager)
    testCreateDeleteCollection(clusterManager)
    testFetchIndex(clusterManager)
    testBackup(clusterManager)

  }

  def testAddRemoveReplica(clusterManager: ClusterManager): Unit = {
    val state = clusterManager.currentState
    val nodesWithNewCollection = state.replicasFor(newCollectionName).map(_.node).distinct
    val nodesNotInNewCollection = state.liveNodes.filterNot(nodesWithNewCollection.contains(_))
    val sliceToReplicate = state.replicasFor(newCollectionName).head.sliceName
    val replicateToNode = nodesNotInNewCollection.head

    assertTrue(Operation(Seq(AddReplica(newCollectionName, sliceToReplicate, replicateToNode))).execute(cloudClient))
    assertFalse(Operation(Seq(AddReplica("boguscollectionname", sliceToReplicate, replicateToNode))).execute(cloudClient))
    assertFalse(Operation(Seq(AddReplica(newCollectionName, "bogusSliceName", replicateToNode))).execute(cloudClient))
    assertFalse(Operation(Seq(AddReplica(newCollectionName, sliceToReplicate, "bogusNodeName"))).execute(cloudClient))

    val newState = clusterManager.currentState
    val replicatedSlice = newState.replicasFor(newCollectionName, sliceToReplicate)
    assertTrue(
      "can delete one replica",
      Operation(Seq(DeleteReplica(newCollectionName, sliceToReplicate, replicatedSlice.filter(_.node == replicateToNode).head.node))).execute(cloudClient)
    )
    assertFalse(
      "can't delete all replicas",
      Operation(Seq(DeleteReplica(newCollectionName, sliceToReplicate, replicatedSlice.filter(_.node != replicateToNode).head.node))).execute(cloudClient)
    )
  }

  def testAlias(clusterManager: ClusterManager): Unit = {
    assertTrue("alias w/one collection", Operation(Seq(UpdateAlias("newAlias", Seq(newCollectionName)))).execute(cloudClient))
    assertTrue("alias w/two collections", Operation(Seq(UpdateAlias("newAlias", Seq(newCollectionName, oldCollectionName)))).execute(cloudClient))
    assertFalse("alias to bogus collection", Operation(Seq(UpdateAlias("newAlias", Seq("boguscollectionname")))).execute(cloudClient))
    assertTrue("delete alias", Operation(Seq(DeleteAlias("newAlias"))).execute(cloudClient))
    assertFalse("can't delete an alias twice", Operation(Seq(DeleteAlias("newAlias"))).execute(cloudClient))
  }

  def testCreateDeleteCollection(clusterManager: ClusterManager): Unit = {
    val numSlices = 2
    val state = clusterManager.currentState
    val targetNode = state.liveNodes.head
    // all slices on one node
    val createAction = CreateCollection("testcollection", numSlices, "conf1", Some(numSlices), None, Some(Seq(targetNode)))
    assertTrue(Operation(Seq(createAction)).execute(cloudClient))

    val nodesWithCollection = clusterManager.currentState.nodesWithCollection("testcollection")
    assertEquals(1, nodesWithCollection.size)
    assertTrue(nodesWithCollection.contains(targetNode))

    assertTrue(Operation(Seq(DeleteCollection("testcollection"))).execute(cloudClient))
    assertFalse(clusterManager.currentState.collections.contains("testcollection"))
  }

  def testFetchIndex(clusterManager: ClusterManager): Unit = {
    val state = clusterManager.currentState
    val initialNode = state.liveNodes.head
    val targetNode = state.liveNodes.tail.head
    val createFullCollectionAction = CreateCollection("fetchfrom", 1, "conf1", Some(1), None, Some(Seq(initialNode)))
    assertTrue(Operation(Seq(createFullCollectionAction)).execute(cloudClient))
    val createEmptyCollectionAction = CreateCollection("fetchinto", 1, "conf1", Some(1), None, Some(Seq(targetNode)))
    assertTrue(Operation(Seq(createEmptyCollectionAction)).execute(cloudClient))

    def collectionSize(coll: String) = {
      val defaultCollection = cloudClient.getDefaultCollection
      cloudClient.setDefaultCollection(coll)

      val countQuery = new SolrQuery()
      countQuery.setQuery( "*:*" )
      val indexedCount = cloudClient.query(countQuery).getResults.getNumFound

      cloudClient.setDefaultCollection(defaultCollection)
      indexedCount
    }

    val numDocs = 3
    val docs = Range(0, numDocs).map{ _ =>
      val doc = new SolrInputDocument
      addRandFields(doc)
      doc.addField("id", Random.nextInt())
      doc
    }.asJava
    val req = new UpdateRequest()
    req.add(docs)
    val defaultCollection = cloudClient.getDefaultCollection
    cloudClient.setDefaultCollection("fetchfrom")
    cloudClient.request(req)
    cloudClient.commit()
    cloudClient.setDefaultCollection(defaultCollection)
    assertEquals(numDocs, collectionSize("fetchfrom"))

    // url-encode any slashes in the hostContext besides the first
    val hostContext = "/" + java.net.URLEncoder.encode(System.getProperty("hostContext").tail, "UTF-8")
    println("HostContext: " + System.getProperty("hostContext") + " becomes " + hostContext)
    println("targetNode: " + targetNode)
    assertTrue(Operation(Seq(FetchIndex("fetchfrom_shard1_replica1", "fetchinto_shard1_replica1", targetNode, hostContext))).execute(cloudClient))
    assertEquals(numDocs, collectionSize("fetchinto"))

    // clean up
    assertTrue(Operation(Seq(DeleteCollection("fetchfrom"))).execute(cloudClient))
    assertTrue(Operation(Seq(DeleteCollection("fetchinto"))).execute(cloudClient))
  }

  def testBackup(clusterManager: ClusterManager): Unit = {
    val backupDir = LuceneTestCase.createTempDir("backups")

    assertTrue(Operation(Seq(BackupIndex(
      "collection2_shard1_replica1",
      backupDir.toAbsolutePath.toString
    ))).execute(cloudClient))

    assertTrue(backupDir.toFile.listFiles().exists(f => f.isDirectory && f.toString.contains("snapshot")))
  }

}
