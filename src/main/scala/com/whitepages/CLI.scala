package com.whitepages

import org.apache.solr.client.solrj.impl.CloudSolrServer
import com.whitepages.cloudmanager.operation.{Operations, Operation}
import com.whitepages.cloudmanager.action._
import com.whitepages.cloudmanager.{ManagerConsoleLogging, ManagerSupport, ManagerException}
import com.whitepages.cloudmanager.action.UpdateAlias
import scala.Some
import com.whitepages.cloudmanager.state.ClusterManager
import com.whitepages.cloudmanager.action.DeleteCollection
import com.whitepages.cloudmanager.action.DeleteReplica
import org.apache.solr.common.cloud.ZkStateReader
import scala.collection.JavaConverters._
import org.slf4j.LoggerFactory
import org.apache.log4j.Level


object CLI extends App with ManagerSupport {
  case class CLIConfig(
                        zk: String = "",
                        mode: String = "clusterstatus",
                        collection: String = "",
                        slicesPerNode: Int = 0,
                        wipe: Boolean = false,
                        confirm: Boolean = true,
                        slice: String = "",
                        node: String = "",
                        safetyFactor: Int = 1,
                        alias: String = "",
                        numSlices: Int = 0,
                        configName: String = "",
                        maxShardsPerNode: Option[Int] = None,
                        replicationFactor: Option[Int] = None,
                        createNodeSet: Option[Seq[String]] = None,
                        alternateHost: String = "",
                        outputLevel: Level = Level.INFO
  )
  val cliParser = new scopt.OptionParser[CLIConfig]("zk_monitor") {
    help("help")text("print this usage text")
    opt[String]('z', "zk") required() action { (x, c) => { c.copy(zk = x) } } text("Zookeeper connection string, including any chroot path")
    opt[Unit]("confirm") optional() action { (_, c) =>
      c.copy(confirm = false) } text("Assume the operation is confirmed, don't prompt")
    opt[Unit]("debug") optional() action { (_, c) =>
      c.copy(outputLevel = Level.DEBUG) } text("debug output")
    opt[Unit]("quiet") optional() action { (_, c) =>
      c.copy(outputLevel = Level.WARN) } text("less output")
    cmd("clusterstatus") action { (_, c) =>
      c.copy(mode = "clusterstatus") } text("Print current cluster status")
    cmd("populate") action { (_, c) =>
      c.copy(mode = "populate") } text("populate a cluster from a given node, presumed to be an indexer") children(
        opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The collection to populate across the cluster"),
        opt[Int]("slicesPerNode") required() action { (x, c) => { c.copy(slicesPerNode = x) } } text("The desired number of slices on each node"),
        opt[Unit]("wipe") optional() action { (_, c) =>
          c.copy(wipe = true) } text("Wipe the originating node after we're done populating the cluster from it")
      )
    cmd("fill") action { (_, c) =>
      c.copy(mode = "fill") } text("Uses available/unused nodes to add more replicas") children(
        opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The name of the collection to fill out")
      )
    cmd("addreplica") action { (_, c) =>
      c.copy(mode = "addreplica") } text("Uses available/unused nodes to add more replicas") children(
        opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The name of the collection to add the replica for"),
        opt[String]("slice") required() action { (x, c) => { c.copy(slice = x) } } text("The name of the slice to add the replica for"),
        opt[String]("node") required() action { (x, c) => { c.copy(node = x) } } text("The node the replica should be added to")
      )
    cmd("deletereplica") action { (_, c) =>
      c.copy(mode = "deletereplica") } text("delete a specific replica") children(
        opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The collection of the replica"),
        opt[String]("slice") required() action { (x, c) => { c.copy(slice = x) } } text("The slice name of the replica"),
        opt[String]("node") required() action { (x, c) => { c.copy(node = x) } } text("The node the replica resides on"),
        opt[Int]("safetyFactor") optional() action { (x, c) => { c.copy(safetyFactor = x) } } text("Fail if this action would result in fewer total replicas than this. Default 1.")
      )
    cmd("alias") action { (_, c) =>
      c.copy(mode = "alias") } text("Create an alias, or move the pointer if it already exists") children(
        opt[String]('a', "alias") required() action { (x, c) => { c.copy(alias = x) } } text("The name of the desired alias"),
        opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("Comma-delinated collection names for this alias to point to")
      )
    cmd("deletealias") action { (_, c) =>
      c.copy(mode = "deletealias") } text("Create an alias, or move the pointer if it already exists") children(
        opt[String]('a', "alias") required() action { (x, c) => { c.copy(alias = x) } } text("The name of the alias to delete")
      )
    cmd("deletecollection") action { (_, c) =>
      c.copy(mode = "deletecollection") } text("Deletes the specified collection") children(
        opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The name of the collection to delete")
      )
    cmd("createcollection") action { (_, c) =>
      c.copy(mode = "createcollection") } text("Creates the specified collection") children(
        opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The name of the collection to create"),
        opt[Int]("slices") required() action { (x, c) => { c.copy(numSlices = x) } } text("The desired number of slices"),
        opt[String]("config") required() action { (x, c) => { c.copy(configName = x) } } text("The name of the config to use for this collection"),
        opt[Int]("maxSlicesPerNode") optional() action { (x, c) => { c.copy(maxShardsPerNode = Some(x)) } } text("When auto-assigning slices, don't allow more than this per node. Default 1"),
        opt[Int]("replicationFactor") optional() action { (x, c) => { c.copy(replicationFactor = Some(x)) } } text("The desired number of replicas (1-based, default 1)"),
        opt[String]("nodes") optional() action { (x, c) => { c.copy(createNodeSet = Some(x.split(","))) } } text("Comma-delineated list of nodes to limit this collection to. (Default all)")
      )
    cmd("copy") action { (_, c) =>
      c.copy(mode = "copy") } text("Copies a collection from one cluster to another. The collection you're copying into MUST pre-exist, be empty and have the same number of slices.") children(
        opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The name of the collection to copy"),
        opt[String]("copyFrom") required() action { (x, c) => { c.copy(alternateHost = x) } } text("A reference to a node (any node) in the cluster to copy from, ie 'foo.QA.com:8983'")
      )
    checkConfig{
      c =>
        if (c.zk.isEmpty) failure("provide a zookeeper connection string, with port and (optional) chroot")
        else success
    }
  }

  cliParser.parse(args, CLIConfig()).fold({
    // argument error, the parser should have already informed the user
  })({
    config =>
      implicit val clusterManager = new ClusterManager(config.zk)
      val startState = clusterManager.currentState
      ManagerConsoleLogging.setLevel(config.outputLevel)

      var success = false
      try {

        // get the requested operation
        val operation: Operation = config.mode match {
          case "clusterstatus" => {
            clusterManager.printAliases
            startState.printReplicas()
            Operation.empty
          }
          case "populate" => {
            val nodesWithCollection = startState.nodesWithCollection(config.collection)
            if (nodesWithCollection.size > 1) {
              comment.warn("It doesn't look like we're populating from a single indexer node, as expected")
              comment.warn(s"Collection ${config.collection} appears to exist on the following nodes: ${nodesWithCollection.mkString(", ")}")
              exit(1)
            }
            val originatingNode = nodesWithCollection.head // highlander

            val populationOperation = Operations.populateCluster(clusterManager, config.collection, config.slicesPerNode)
            val wipeOperation =
              if (config.wipe) Operations.wipeCollectionFromNode(clusterManager, config.collection, originatingNode)
              else Operation.empty

            populationOperation ++ wipeOperation
          }
          case "fill" => {
            Operations.fillCluster(clusterManager, config.collection)
          }
          case "addreplica" => {
            Operation(Seq(AddReplica(config.collection, config.slice, config.node)))
          }
          case "deletereplica" => {
            Operation(Seq(DeleteReplica(config.collection, config.slice, startState.canonicalNodeName(config.node), config.safetyFactor)))
          }
          case "alias" => {
            val collections = config.collection.split(",")
            Operation(Seq(UpdateAlias(config.alias, collections)))
          }
          case "deletealias" => {
            Operation(Seq(DeleteAlias(config.alias)))
          }
          case "deletecollection" => {
            Operation(Seq(DeleteCollection(config.collection)))
          }
          case "createcollection" => {

            // CreateCollection checks for this, but might as well check this before we get started too
            if (!CreateCollection.configExists(clusterManager, config.configName)) {
              exit(1)
            }

            val normalizedNodes = config.createNodeSet.map(_.map(startState.canonicalNodeName))
            Operation(Seq(CreateCollection(
              config.collection,
              config.numSlices,
              config.configName,
              config.maxShardsPerNode,
              config.replicationFactor,
              normalizedNodes
            )))
          }
          case "copy" => {
            Operations.deployFromAnotherCluster(clusterManager, config.collection, config.alternateHost)
          }
        }

        // get user confirmation, if necessary
        if (config.confirm && operation.nonEmpty) {
          comment.warn(operation.prettyPrint)
          val input = readLine("Seem reasonable? [y]> ")
          if (input.toLowerCase.contains("n")) {
            comment.warn("Aborting.")
            exit(1)
          }
        }

        //execute the operation
        success = operation.execute(clusterManager)
      } catch {
        case m: ManagerException => comment.warn(m.getMessage)
      }
      exit(if (success) 0 else 1)
  })

  /**
   * sys.addShutdownHook wasn't working well for this, so kludge something else to handle clean client shutdown
   * @param status Command-line result code (so 0 is success, anything else is a failure)
   */
  def exit(status: Int)(implicit clusterManager: ClusterManager): Unit = {
    clusterManager.shutdown()
    comment.warn(if (status == 0) "SUCCESS" else "FAILURE")
    sys.exit(status)
  }
}
