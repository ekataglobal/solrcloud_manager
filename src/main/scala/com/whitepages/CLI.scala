package com.whitepages

import java.net.InetAddress
import java.nio.file.{Paths, Path}

import com.whitepages.cloudmanager.operation.Operations.CloneCollectionOverrides
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
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal
import scala.annotation.tailrec
import scala.collection._


object CLI extends App with ManagerSupport {
  case class CLIConfig(
                        zk: String = "",
                        mode: String = "clusterstatus",
                        collection: String = "",
                        slicesPerNode: Int = 0,
                        wipe: Boolean = false,
                        prompt: Boolean = true,
                        slice: String = "",
                        node: String = "",
                        node2: String = "",
                        safetyFactor: Int = 1,
                        alias: String = "",
                        numSlices: Int = 1,
                        configName: String = "",
                        maxShardsPerNode: Option[Int] = None,
                        replicationFactor: Option[Int] = None,
                        nodeSet: Option[Seq[String]] = None,
                        asyncOps: Boolean = false,
                        alternateHost: String = "",
                        timeout: Duration = Duration.Inf,
                        strict: Boolean = false,
                        outputLevel: Level = Level.INFO,
                        parallelOps: Boolean = false,
                        backupLimit: Int = 2,
                        backupDir: String = "",
                        restoreCollection: Option[String] = None,
                        fromCollection: String = "",
                        altClusterRef: String = "",
                        confirmOp: Boolean = true,
                        localPath: Path = Paths.get("")
  )
  val cliParser = new scopt.OptionParser[CLIConfig]("zk_monitor") {
    note("The following options are available for all commands")
    help("help")text("print this usage text")
    opt[String]('z', "zk") required() action { (x, c) => { c.copy(zk = x) } } text("Zookeeper connection string, including any chroot path. *Required for all commands.*")
    opt[Unit]("confirm") optional() action { (_, c) =>
      c.copy(prompt = false) } text("Assume the operation is confirmed, don't prompt")
    opt[Unit]('d', "debug") optional() action { (_, c) =>
      c.copy(outputLevel = Level.DEBUG) } text("debug output")
    opt[Unit]('q', "quiet") optional() action { (_, c) =>
      c.copy(outputLevel = Level.WARN) } text("less output")
    note("\n------View commands-----\n")
    cmd("clusterstatus") action { (_, c) =>
      c.copy(mode = "clusterstatus") } text("Print current cluster status. This is the default command.")
    cmd("waitactive") action { (_, c) =>
      c.copy(mode = "waitactive") } text("Doesn't return until a given node is fully active and participating in the cluster. Useful for preventing other things until a node is ready.") children(
      opt[String]('n', "node") optional() action { (x, c) => { c.copy(node = x) } } text("The name of one or more (comma-delinated) nodes that should be active. Default: localhost."),
      opt[Int]("timeout") optional() action { (x, c) => { c.copy(timeout = x.seconds) } } text("How long (seconds) to wait for the node to be fully active before failing. Default: Infinite."),
      opt[Unit]("strict") optional() action { (_, c) => { c.copy(strict = true) } } text("Whether to fail if any node names couldn't be found. Default false.")
      )
    note("\n------Collections API------\n")
    cmd("createcollection") action { (_, c) =>
      c.copy(mode = "createcollection") } text("Creates the specified collection") children(
      opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The name of the collection to create"),
      opt[Int]("slices") optional() action { (x, c) => { c.copy(numSlices = x) } } text("The desired number of slices. Default 1"),
      opt[String]("config") required() action { (x, c) => { c.copy(configName = x) } } text("The name of the config to use for this collection"),
      opt[Int]("maxSlicesPerNode") optional() action { (x, c) => { c.copy(maxShardsPerNode = Some(x)) } } text("When auto-assigning slices, don't allow more than this per node. Default 1"),
      opt[Int]("replicationFactor") optional() action { (x, c) => { c.copy(replicationFactor = Some(x)) } } text("The desired number of replicas (1-based, default 1)"),
      opt[String]("nodes") optional() action { (x, c) => { c.copy(nodeSet = Some(x.split(","))) } } text("Comma-delineated list of nodes to limit this collection to. Default: all"),
      opt[Unit]("async") optional() action { (_, c) =>
        c.copy(asyncOps = true) } text("Submit the creation request as an async job. This hides error messages, but protects against timeouts. Default: false")
      )
    cmd("deletecollection") action { (_, c) =>
      c.copy(mode = "deletecollection") } text("Deletes the specified collection") children(
      opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The name of the collection to delete")
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
      opt[String]("slice") required() action { (x, c) => { c.copy(slice = x) } } text("The slice name of the replica, ie 'shard1'"),
      opt[String]("node") required() action { (x, c) => { c.copy(node = x) } } text("The node the replica resides on"),
      opt[Int]("safetyFactor") optional() action { (x, c) => { c.copy(safetyFactor = x) } } text("Fail if this action would result in fewer total replicas than this. Default 1.")
      )
    cmd("alias") action { (_, c) =>
      c.copy(mode = "alias") } text("Create an alias, or move the pointer if it already exists") children(
      opt[String]('a', "alias") required() action { (x, c) => { c.copy(alias = x) } } text("The name of the desired alias"),
      opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("Comma-delinated collection names for this alias to point to")
      )
    cmd("deletealias") action { (_, c) =>
      c.copy(mode = "deletealias") } text("Delete an existing alias") children(
      opt[String]('a', "alias") required() action { (x, c) => { c.copy(alias = x) } } text("The name of the alias to delete")
      )
    note("\n------Cluster manipulation------\n")
    cmd("clean") action { (_, c) =>
      c.copy(mode = "clean") } text("Remove all replicas from the given (comma-delinated) nodes") children(
        opt[String]('c', "collection") optional() action { (x, c) => { c.copy(collection = x) } } text("Limit removals to this collection. Default: all collections"),
        opt[String]("nodes") required() action { (x, c) => { c.copy(nodeSet = Some(x.split(","))) } } text("Comma-delineated list of nodes to remove replicas from"),
        opt[Int]("safetyFactor") optional() action { (x, c) => { c.copy(safetyFactor = x) } } text("Fail any delete that would result in fewer total replicas than this. Default 1.")
      )
    cmd("cleancollection") action { (_, c) =>
      c.copy(mode = "cleancollection") } text("Remove any non-active replicas from the cluster. (Warning: Including those in RECOVERING)") children(
      opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The collection to clean")
      )
    cmd("clone") action { (_, c) =>
      c.copy(mode = "clone") } text("Adds all replicas on a given node to another node") children(
        opt[String]("from") required() action { (x, c) => { c.copy(node = x) } } text("Node to clone"),
        opt[String]("onto") required() action { (x, c) => { c.copy(node2 = x) } } text("Node to clone onto"),
        opt[Unit]("parallel") optional() action { (x, c) => { c.copy(parallelOps = true) } } text("Create all replicas at once, instead of one-at-a-time. Default: false")
      )
    cmd("migratenode") action { (_, c) =>
      c.copy(mode = "migrate") } text("Adds all replicas on a given node to another node, then removes those replicas from the original node") children(
      opt[String]("from") required() action { (x, c) => { c.copy(node = x) } } text("Node to clone"),
      opt[String]("onto") required() action { (x, c) => { c.copy(node2 = x) } } text("Node to clone onto")
      )
    cmd("copy") action { (_, c) =>
      c.copy(mode = "copy") } text("(DEPRECATED) Use 'copycollection' instead") children(
      opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The name of the collection to copy"),
      opt[String]("copyFrom") required() action { (x, c) => { c.copy(alternateHost = x) } } text("A reference to a host (any host) in the cluster to copy FROM, ie 'foo.QA.com:8983/solr'")
      )
    cmd("copycollection") action { (_, c) =>
      c.copy(mode = "copycollection") } text("(EXPERIMENTAL) Copies one collection's index data into another. The collection you're copying into MUST pre-exist in the cluster referenced with -z, be empty, and have the same number of slices. The replicationFactor can be different.") children(
      opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The name of the collection to copy INTO"),
      opt[String]("fromCollection") required() action { (x, c) => { c.copy(fromCollection = x) } } text("The name of the collection to copy FROM"),
      opt[String]("fromCluster") optional() action { (x, c) => { c.copy(altClusterRef = x) } } text("The ZK reference for the cluster you're copying FROM. Default: The same cluster you're copying INTO. (-z)"),
      opt[Unit]("skipCheck") optional() action { (_, c) => { c.copy(confirmOp = false) } } text("Skip the DocCount check after copy. Use if the collection you're copying from is taking updates. Default: false")
      )
    cmd("clonecollection") action { (_, c) =>
      c.copy(mode = "clonecollection") } text("Creates a new empty collection using another collection (potentially in another cluster) as a template") children(
      opt[String]("fromCollection") required() action { (x, c) => { c.copy(fromCollection = x) } } text("The name of the collection to clone"),
      opt[String]("fromCluster") optional() action { (x, c) => { c.copy(altClusterRef = x) } } text("The ZK reference for the cluster containing the collection you're cloning. Default: The same cluster you're cloning into. (-z)"),
      opt[String]('c', "collection") optional() action { (x, c) => { c.copy(collection = x) } } text("The name of the new collection. Default: the name of the cloned collection"),
      opt[String]("config") optional() action { (x, c) => { c.copy(configName = x) } } text("A config name to use for this collection. Default: the config name from the cloned collection"),
      opt[Int]("maxSlicesPerNode") optional() action { (x, c) => { c.copy(maxShardsPerNode = Some(x)) } } text("The number of shards per node. Default: the max shards per node of the cloned collection"),
      opt[Int]("replicationFactor") optional() action { (x, c) => { c.copy(replicationFactor = Some(x)) } } text("A replacement replication factor. Default: the replication factor of the cloned collection"),
      opt[String]("nodes") optional() action { (x, c) => { c.copy(nodeSet = Some(x.split(","))) } } text("Comma-delineated list of nodes to limit this new collection to. Default: all")
      )
    cmd("populate") action { (_, c) =>
      c.copy(mode = "populate") } text("(EXPERIMENTAL) populate a cluster from a given node, presumed to be an indexer") children(
        opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The collection to populate across the cluster"),
        opt[Int]("slicesPerNode") required() action { (x, c) => { c.copy(slicesPerNode = x) } } text("The desired number of slices on each node"),
        opt[Unit]("wipe") optional() action { (_, c) =>
          c.copy(wipe = true) } text("Wipe the originating node after we're done populating the cluster from it. Default: false")
      )
    cmd("fill") action { (_, c) =>
      c.copy(mode = "fill") } text("Uses available/unused nodes to add more replicas") children(
        opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The name of the collection to fill out"),
        opt[String]("nodes") optional() action { (x, c) => { c.copy(nodeSet = Some(x.split(","))) } } text("Comma-delineated list of nodes to fill into. Default: all"),
        opt[Unit]("parallel") optional() action { (x, c) => { c.copy(parallelOps = true) } } text("Create all replicas without waiting for each to fully replicate. Default: false"),
        opt[Int]("maxSlicesPerNode") optional() action { (x, c) => { c.copy(maxShardsPerNode = Some(x)) } } text("The number of shards per node. Default: the max number of shards on any current node")
      )
    note("\n-----Backup commands-----\n")
    cmd("backupindex") action { (_, c) =>
      c.copy(mode = "backupindex") } text("Triggers a backup request for a given collection") children(
      opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The name of the collection to back up"),
      opt[String]("dir") required() action { (x, c) => { c.copy(backupDir = x) } } text("The base directory on each node to put the backup in. The collection and slice names will be appended. Typically a shared filesystem across all nodes."),
      opt[Int]("keep") optional() action { (x, c) => { c.copy(backupLimit = x) } } text("The number of backups for a given node/core to keep. Default 2."),
      opt[Unit]("parallel") optional() action { (x, c) => { c.copy(parallelOps = true) } } text("Don't wait to confirm each replica backup succeeds. Default: false")
      )
    cmd("restoreindex") action { (_, c) =>
      c.copy(mode = "restoreindex") } text("Loads a backup into an existing collection") children(
      opt[String]('c', "collection") required() action { (x, c) => { c.copy(collection = x) } } text("The name of the collection to restore into"),
      opt[String]("dir") required() action { (x, c) => { c.copy(backupDir = x) } } text("The base directory on the nodes where the backup index data is saved. Typically a shared filesystem across all nodes."),
      opt[String]("restoreFrom") optional() action { (x, c) => { c.copy(restoreCollection = Some(x)) } } text("Restore this collection name's index data. Default: the name of the collection (-c)"),
      opt[Unit]("parallel") optional() action { (x, c) => { c.copy(parallelOps = true) } } text("Don't wait to confirm each replica restore succeeds. Default: false")
      )
    note("\n------Config commands------\n")
    cmd("upconfig") action { (_, c) =>
      c.copy(mode = "upconfig") } text("Upload a configset to ZK") children(
      opt[String]("dir") required() action { (x, c) => { c.copy(localPath = Paths.get(x)) } } text("The local directory containing the config files"),
      opt[String]("config") optional() action { (x, c) => { c.copy(configName = x) } } text("The name of the configset. Default: The name of the directory")
      )
    cmd("downconfig") action { (_, c) =>
      c.copy(mode = "downconfig") } text("Download a configset to ZK") children(
      opt[String]("dir") required() action { (x, c) => { c.copy(localPath = Paths.get(x)) } } text("The local directory containing the config files"),
      opt[String]("config") required() action { (x, c) => { c.copy(configName = x) } } text("The name of the configset")
      )
    cmd("rmconfig") action { (_, c) =>
      c.copy(mode = "rmconfig") } text("Delete a configset from ZK") children(
      opt[String]("config") required() action { (x, c) => { c.copy(configName = x) } } text("The name of the configset to delete")
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

      ManagerConsoleLogging.setLevel(config.outputLevel)
      var success = false

      try {
        val clusterManager = RegisteredClusterManagers.get(config.zk)
        val startState = clusterManager.currentState

        // get the requested operation
        val operation: Operation = config.mode match {
          case "clusterstatus" => {
            clusterManager.printClusterVersion()
            clusterManager.printOverseer()
            clusterManager.printAliases()
            clusterManager.printConfigs()
            startState.printReplicas()
            Operation.empty
          }
          case "clean" => {
            // TODO: The Option type (or lack thereof) of these config variables is being abused here.
            val deletes = for (node <- config.nodeSet.get) yield {
              Operations.wipeNode(
                clusterManager,
                startState.canonicalNodeName(node, true),
                if (config.collection.isEmpty) None else Some(config.collection),
                config.safetyFactor
              )
            }
            deletes.fold(Operation.empty)(_ ++ _)
          }
          case "clone" => {
            Operations.cloneReplicas(
              clusterManager,
              startState.canonicalNodeName(config.node, true),
              startState.canonicalNodeName(config.node2),
              !config.parallelOps
            )
          }
          case "migrate" => {
            val from = startState.canonicalNodeName(config.node, true)
            val onto = startState.canonicalNodeName(config.node2)

            Operations.cloneReplicas(clusterManager, from, onto) ++ Operations.wipeNode(clusterManager, from)
          }
          case "cleancollection" => {
            Operations.cleanCluster(clusterManager, config.collection)
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
            val normalizedNodes = config.nodeSet.map(_.map(name => startState.canonicalNodeName(name)))
            Operations.fillCluster(
              clusterManager, config.collection, normalizedNodes, !config.parallelOps, config.maxShardsPerNode
            )
          }
          case "addreplica" => {
            Operation(Seq(AddReplica(config.collection, config.slice, startState.canonicalNodeName(config.node))))
          }
          case "deletereplica" => {
            Operation(Seq(DeleteReplica(config.collection, config.slice, startState.canonicalNodeName(config.node, allowOfflineReferences = true), config.safetyFactor)))
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
            if (!clusterManager.configExists(config.configName)) {
              comment.warn(s"The specified config '${config.configName}' couldn't be found in ZK. Known configs are: ${clusterManager.configs.mkString(", ")}")
              exit(1)
            }

            val normalizedNodes = config.nodeSet.map(_.map(name => startState.canonicalNodeName(name)))
            Operation(Seq(CreateCollection(
              config.collection,
              config.numSlices,
              config.configName,
              config.maxShardsPerNode,
              config.replicationFactor,
              normalizedNodes,
              config.asyncOps
            )))
          }
          case "copy" => {
            comment.warn("'copy' is deprecated, use 'copycollection' instead")

            if (!Conditions.collectionExists(config.collection)(clusterManager.currentState)) {
              comment.warn(s"Can't copy into non-existent target collection ${config.collection}")
              exit(1)
            }

            Operations.deployFromAnotherCluster(clusterManager, config.collection, config.alternateHost)
          }
          case "copycollection" => {
            val fromClusterManager =
              if (config.altClusterRef.isEmpty) clusterManager
              else RegisteredClusterManagers.get(config.altClusterRef)

            Operations.copyCollection(clusterManager, config.collection, fromClusterManager, config.fromCollection, config.confirmOp)
          }
          case "clonecollection" => {
            val overrides = CloneCollectionOverrides(
              configName = if (config.configName.isEmpty) None else Some(config.configName),
              maxSlicesPerNode = config.maxShardsPerNode,
              replicationFactor = config.replicationFactor,
              createNodeSet = config.nodeSet.map(_.map(name => startState.canonicalNodeName(name)))
            )
            val fromClusterManager =
              if (config.altClusterRef.isEmpty) clusterManager
              else RegisteredClusterManagers.get(config.altClusterRef)

            Operations.cloneCollection(
              clusterManager, if (config.collection.isEmpty) config.fromCollection else config.collection,
              fromClusterManager, config.fromCollection,
              overrides
            )
          }
          case "waitactive" => {
            val nodeNames: List[String] =
              if (config.node.isEmpty) List(java.net.InetAddress.getLocalHost.getHostName)
              else config.node.split(",").toList
            val waitNodes = nodeNames.foldLeft(List.empty[String])( (acc, nodeName) => {
              val canonicalName = Try(startState.canonicalNodeName(nodeName, allowOfflineReferences = true))
              if (canonicalName.isSuccess)
                canonicalName.get :: acc
              else {
                comment.warn("Could not determine node name from " + nodeName)
                if (config.strict) exit(1)
                acc
              }
            })

            val startTime = System.nanoTime()
            var fullyActive = false
            do  {
              val nodeReplicas = clusterManager.currentState.allReplicas.filter(replica => waitNodes.contains(replica.node))
              val replicaCount = nodeReplicas.size
              val activeReplicaCount = nodeReplicas.count(_.active)
              comment.info(s"$activeReplicaCount of $replicaCount replicas are active")
              if (replicaCount > activeReplicaCount) {
                if ((System.nanoTime() - startTime).nanos > config.timeout) exit(1)
                Thread.sleep(5000)
              }
              else
                fullyActive = true

            } while (!fullyActive)

            Operation.empty
          }
          case "backupindex" => {
            Operations.backupCollection(
              clusterManager,
              config.collection,
              config.backupDir,
              config.backupLimit,
              config.parallelOps
            )
          }
          case "restoreindex" => {
            Operations.restoreCollection(clusterManager, config.collection, config.backupDir, config.restoreCollection)
          }
          case "upconfig" => {
            val configName =
              if (config.configName.isEmpty) config.localPath.toAbsolutePath.getFileName().toString
              else config.configName
            Operation(List(UploadConfig(config.localPath, configName)))
          }
          case "downconfig" => {
            Operation(List(DownloadConfig(config.localPath, config.configName)))
          }
          case "rmconfig" => {
            Operation(List(DeleteConfig(config.configName)))
          }
        }

        // get user confirmation, if necessary
        if (config.prompt && operation.nonEmpty) {
          comment.warn(operation.prettyPrint)
          val input = scala.io.StdIn.readLine("Seem reasonable? [y]> ")
          if (input.toLowerCase.contains("n")) {
            comment.warn("Aborting.")
            exit(1)
          }
        }

        //execute the operation
        success = operation.execute(clusterManager)
      } catch {
        case m: ManagerException => comment.warn(m.getMessage)
        case NonFatal(e) => comment.warn(getRootCause(e).getMessage)
      }

      if (!success) exit(1)
      comment.info("SUCCESS")
      RegisteredClusterManagers.shutdown()
  })

  /**
   * sys.addShutdownHook doesn't work reliably when running in SBT, so kludge something else to handle clean client shutdown
   * @param status Command-line result code (so 0 is success, anything else is a failure)
   */
  def exit(status: Int): Unit = {
    RegisteredClusterManagers.shutdown()
    comment.info(if (status == 0) "SUCCESS" else "FAILURE")
    sys.exit(status)
  }

  /**
   * Gets the first-order exception for a given exception.
   * It's really mind-boggling to me this isn't part of the Throwable class.
   * @param e Some exception
   * @return The exception at the bottom of the getCause nesting
   */
  @tailrec
  def getRootCause(e: Throwable): Throwable = {
    e.getCause match {
      case null => e
      case e: Throwable => getRootCause(e)
    }
  }

  object RegisteredClusterManagers {
    private var clusterManagers: mutable.Set[ClusterManager] = mutable.Set.empty
    def get(zk: String) = {
      // Stored in a set, which prevents duplicate managers based on the same zk string,
      // but I'd still rather not create a new manager just to throw it away as a dupe
      val managerOpt = clusterManagers.find(_.client.getZkHost == zk)
      managerOpt.getOrElse{
        val c = ClusterManager(zk)
        clusterManagers += c     // stash for later shutdown
        c
      }
    }
    def shutdown() {
      clusterManagers.foreach(_.shutdown())
      clusterManagers.clear()
    }
  }
}
