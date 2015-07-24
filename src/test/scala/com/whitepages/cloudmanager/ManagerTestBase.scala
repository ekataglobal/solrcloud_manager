package com.whitepages.cloudmanager

import org.apache.solr.BaseDistributedSearchTestCase.ShardsFixed
import org.apache.solr.cloud.AbstractFullDistribZkTestBase

import org.junit.runner.RunWith
import org.junit.Assert._
import com.carrotsearch.randomizedtesting.RandomizedRunner
import org.apache.solr.SolrTestCaseJ4
import org.junit.{Test, After}
import org.apache.log4j.{Level, LogManager}
import com.whitepages.cloudmanager.action.DeleteReplica
import com.whitepages.cloudmanager.state.ClusterManager
import com.whitepages.cloudmanager.operation.Operation
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope


@SolrTestCaseJ4.SuppressSSL
@RunWith(classOf[RandomizedRunner])
@ThreadLeakScope(Scope.NONE)  // disable the usual lucene test case leak checking
abstract class ManagerTestBase extends AbstractFullDistribZkTestBase {

  fixShardCount(4)  // four nodes
  val nodeCount = getShardCount            // alias a less confusing name (does NOT include the control node)
  sliceCount = 1  // doesn't really matter, the base class chooses enough replicas to fill the shardcount, which isn't what we want here
  val oldCollectionName = "collection1" // I can't get to AbstractDistribZkTestBase.DEFAULT_COLLECTION, so copied here

  val logSettings = Map(
    "org.apache.solr.schema" -> Level.OFF,
    "org.apache.solr.core.SolrResourceLoader" -> Level.OFF,
    "org.apache.solr.cloud.RecoveryStrategy" -> Level.OFF,
    "org.apache.solr.core" -> Level.ERROR,
    "org.apache.solr.rest" -> Level.ERROR,
    "org.apache.solr.update" -> Level.ERROR,
    "org.apache.solr.cloud" -> Level.ERROR,
    "org.apache.solr" -> Level.WARN,
    "org.apache.zookeeper" -> Level.WARN,
    "org.eclipse.jetty.server" -> Level.WARN
  )

  def logAt(which: String, lvl: Level) = LogManager.getLogger(which).setLevel(lvl)
  def limitLogging =
    logSettings.foreach{ case (where, lvl) => logAt(where, lvl) }
  def stopLogging = {
    logSettings.foreach{ case (where, lvl) => logAt(where, Level.OFF) }
    LogManager.getRootLogger.setLevel(Level.OFF)
  }

  @Override
  @After
  override def setUp(): Unit = {
    limitLogging
    super.setUp()
  }


  @Override
  @After
  override def tearDown(): Unit = {
    stopLogging
    super.tearDown()
  }

  def managerTest(): Unit

  @Test
  @ShardsFixed(num = 4)
  def doTest(): Unit = {
    val clusterManager = new ClusterManager(cloudClient)

    // wait for the cluster to stabilize
    blockUntilStable(clusterManager)
    var state = clusterManager.currentState

    println("\n\n\n\n\n")
    state = clusterManager.currentState
    state.printReplicas
    println("\n\n\n\n\n")
    assertTrue(state.inactiveReplicas.isEmpty)

    // Do the real testing
    managerTest()

    state = clusterManager.currentState
    state.printReplicas
    println("\n\n\n\n\n")
  }

  def blockUntilStable(clusterManager: ClusterManager) = {
    var state = clusterManager.currentState
    var tries = 30
    while (state.inactiveReplicas.nonEmpty && tries > 0) {
      println("Waiting for all replicas to be active")
      Thread.sleep(1000)
      state = clusterManager.currentState
      tries = tries - 1
    }
    assertTrue(tries > 0)
  }

}
