package com.whitepages.cloudmanager.operation

import java.io.File
import java.nio.file.{Files, Path, Paths}

import com.whitepages.cloudmanager.action._
import com.whitepages.cloudmanager.client.SolrCloudVersion
import com.whitepages.cloudmanager.operation.plan.{Assignment, Participation}
import com.whitepages.cloudmanager.state.{ClusterManager, SolrReplica}
import com.whitepages.cloudmanager.{ManagerSupport, OperationsException}

object Operations extends ManagerSupport {

  /**
   * Generates an operation to handle the expected data deployment scheme. So given our expected build strategy:
   * 1. Add an indexer node to the cluster
   * 2. Create a collection using only that node
   * 3. Index data onto that node
   * 4. Replicate the resulting shards across the other nodes in the cluster
   * 5. Remove the indexer from the collection
   * 6. Remove the indexer node from the cluster
   * This call generates an Operation that handles #4-5.
   *
   * @param clusterManager
   * @param collection
   * @param slicesPerNode
   * @return An Operation that populates a cluster from a collection that exists on a single node
   */
  def populateCluster(clusterManager: ClusterManager, collection: String, slicesPerNode: Int): Operation = {
    val state = clusterManager.currentState

    val nodesWithoutCollection = state.liveNodes -- state.nodesWithCollection(collection)
    assert(state.liveNodes.size - nodesWithoutCollection.size == 1, "Should be expanding from a single node into a cluster of nodes")
    val sliceNames = state.replicasFor(collection).map(_.sliceName)
    assert(sliceNames.size % slicesPerNode == 0, "Presume slices can be divided evenly using slicesPerNode")
    assert(sliceNames.size >= slicesPerNode, s"Can't have more slices per node ($slicesPerNode) than the total number of slices (${sliceNames.size})")
    val nodesPerSet = sliceNames.size / slicesPerNode // number of nodes necessary for a complete index
    val replicationFactor = nodesWithoutCollection.size / nodesPerSet
    assert(nodesWithoutCollection.size % nodesPerSet == 0, s"Can make complete replica sets using the available nodes and slicesPerNode")


    val assignments = nodesWithoutCollection.toSeq.zip(List.fill(replicationFactor)(sliceNames).flatten.grouped(slicesPerNode).toList)
    comment.info(s"Populate Operation Found: Available Nodes - ${nodesWithoutCollection.size}, Replication factor - $replicationFactor, nodesPerSet - $nodesPerSet")
    val actions = for {(node, slices) <- assignments
                       slice <- slices} yield AddReplica(collection, slice, node)
    Operation(actions)
  }

  /**
   * Using the maximum number of slices per host for the given collection as a maximum, adds
   * as many replicas as possible given the nodes currently in the cluster. Tries to add replicas
   * in order of the fewest replicas for a given slice.
   * May end up with an unequal number of replicas for each slice in the collection, if the number
   * of nodes doesn't divide evenly.
   *
   * @param clusterManager
   * @param collection
   * @param nodesOpt
   * @param waitForReplication
   * @param maxSlicesPerNodeOpt
   * @param constrainToNodes  when determining replica counts, ony count replicas on the nodes under consideration instead of all nodes
   * @return The corresponding Operation
   */
  def fillCluster(clusterManager: ClusterManager, collection: String,
                  nodesOpt: Option[Seq[String]] = None, waitForReplication: Boolean = true,
                  maxSlicesPerNodeOpt: Option[Int] = None, constrainToNodes: Boolean = false): Operation = {
    val state = clusterManager.currentState

    assert(state.collections.contains(collection), s"Could find collection $collection")

    val nodes = nodesOpt.getOrElse(state.liveNodes).toSet
    val allCurrentReplicas: Seq[SolrReplica] = state.replicasFor(collection)

    val allSlicesForCollection = allCurrentReplicas.map(_.sliceName).toSet
    val consideredReplicas =
      if (constrainToNodes) allCurrentReplicas.filter(replica => nodes.contains(replica.node))
      else allCurrentReplicas

    val allParticipation = Participation.fromReplicas(allSlicesForCollection, allCurrentReplicas)
    val participation = Participation.fromReplicas(allSlicesForCollection, consideredReplicas)

    // If not told otherwise, use the node with the most slices (anywhere) as a limiter for how many slices to allow per node
    val maxSlicesPerNode = maxSlicesPerNodeOpt.getOrElse(allParticipation.slicesPerNode.maxBy(_._2)._2)

    val currentSlots = consideredReplicas.size
    val availableNodes = nodes.intersect(state.liveNodes)  // the nodes list could contain nodes that are down
    val slotDistribution = availableNodes.map(n => (n, maxSlicesPerNode - allCurrentReplicas.count(_.node == n))).toMap

    // maxSlicesPerNode could be more or less than the number of replicas on any node under consideration
    val availableSlots = slotDistribution.values.map(c => Math.max(c, maxSlicesPerNode)).sum - currentSlots
    Operation(
      participation.assignSlots(availableNodes, availableSlots, slotDistribution)
        .map(a => AddReplica(collection, a.slice, a.node, waitForReplication))
    )
  }

  /**
   * Finds all replicas on a given node, and creates the same set of replicas on another given node.
   * The original node is left untouched, but would be an easy target for a "cleanCluster()" if the
   * intention is a migration.
   * @param clusterManager
   * @param from Node name to gather the replica list from
   * @param onto Node name to add the replicas to
   * @param waitForReplication
   * @return
   */
  def cloneReplicas(clusterManager: ClusterManager, from: String, onto: String, waitForReplication: Boolean = true) = {
    val state = clusterManager.currentState
    val replicasToClone = state.allReplicas.filter(_.node == from)
    val existingReplicas = state.allReplicas.filter(_.node == onto)
    val replicasToCreate = replicasToClone.filterNot(r =>
      existingReplicas.exists(e => r.sliceName == e.sliceName && r.collection == e.collection)
    )

    Operation(replicasToCreate.map(replica => AddReplica(replica.collection, replica.sliceName, onto, waitForReplication)))
  }

  /**
   * Removes any inactive replicas for a given collection.
   * A replica could be "inactive" because it's in a bad state, because the hosting node is down, or because the relevant slice
   * A node need not be up for the replica to be removed.
   * Note that SOLR-6072 means any files on the relevent node are NOT deleted on solr < 4.10.
   * @param clusterManager
   * @param collection
   * @return The corresponding Operation
   */
  def cleanCluster(clusterManager: ClusterManager, collection: String) = {
    val state = clusterManager.currentState

    Operation(
      state.inactiveReplicas.filter(_.collection == collection).map(
        (replica) => DeleteReplica(collection, replica.sliceName, replica.node)
      )
    )
  }

  /**
   * Delete all replicas in all collections from the given node
   * @param clusterManager
   * @param node
   * @return The corresponding Operation
   */
  def wipeNode(clusterManager: ClusterManager, node: String, collectionOpt: Option[String] = None, safetyFactor: Int = 1): Operation = {
    val state = clusterManager.currentState
    val replicasOnNode = collectionOpt.map(collection => state.allReplicas.filter(_.collection == collection))
      .getOrElse(state.allReplicas).filter(_.node == node)

    Operation(replicasOnNode.map( (replica) => DeleteReplica(replica.collection, replica.sliceName, replica.node, safetyFactor)) )
  }

  /**
   * Delete all replicas for a given collection from the given node
   * @param clusterManager
   * @param collection
   * @param node
   * @return The corresponding Operation
   */
  def wipeCollectionFromNode(clusterManager: ClusterManager, collection: String, node: String): Operation = {
    wipeNode(clusterManager, node, Some(collection))
  }

  /**
   * Currently uses coreadmin's fetchindex command even for populating replicas once the leader has been
   * updated. Might be able to do something like this instead:
   * http://10.8.100.42:7575/solr/admin/cores?action=REQUESTRECOVERY&core=collection1_shard1_replica1
   * @param clusterManager
   * @param collection
   * @param deployFrom
   * @return
   */
  def deployFromAnotherCluster(clusterManager: ClusterManager, collection: String, deployFrom: String): Operation = {
    def firstCore(coreName: String) = coreName.replaceAll("""replica\d""", "replica1")

    val state = clusterManager.currentState
    val replicaGroup = state.replicasFor(collection).groupBy(_.sliceName).values.toList.sortBy(_.head.core)
    val operations = for { replicas <- replicaGroup } yield {
      val (leader, copies) = replicas.partition(_.leader)
      Operation(
        leader.flatMap( (r) =>
          FetchIndex(firstCore(r.core), r.core, deployFrom) +: copies.map( (c) => FetchIndex(r.core, c.core, r.url))
        )
      )
    }
    operations.fold(Operation.empty)(_ ++ _)
  }

  def copyCollection(targetClusterManager: ClusterManager, targetCollection: String,
                     fromClusterManager: ClusterManager, fromCollection: String,
                     confirm: Boolean = true): Operation = {
    val targetState = targetClusterManager.currentState
    val fromState = fromClusterManager.currentState
    val targetLeaders = targetState.liveReplicasFor(targetCollection).filter(_.leader).sortBy(_.sliceName)
    val fromLeaders = fromState.liveReplicasFor(fromCollection).filter(_.leader).sortBy(_.sliceName)

    if (!targetState.collections.contains(targetCollection))
      throw new OperationsException(targetCollection + " could not be found in the cluster at " + targetClusterManager.client.getZkHost)
    if (!fromState.collections.contains(fromCollection))
      throw new OperationsException(fromCollection + " could not be found in the cluster at " + fromClusterManager.client.getZkHost)

    if(targetLeaders.size != fromLeaders.size) {
      val errMsg =
        s"Target collection $targetCollection has ${targetLeaders.size} shards, " +
        s"but the collection you're copying from ($fromCollection) has ${fromLeaders.size} shards! " +
        s"The shard count and hashing strategy MUST match to copy a collection."
      throw new OperationsException(errMsg)
    }

    val replicaPairs = targetLeaders zip fromLeaders

    if (replicaPairs.exists( pair => pair._1.sliceName != pair._2.sliceName)) {
      comment.warn("Slice names don't match! Lexical sorting will be used for the slice mapping between the collections, so inspect the following carefully.")
      val pairFormat = s"%-20s => %-20s"
      comment.warn(pairFormat.format(fromCollection, targetCollection))
      replicaPairs.foreach{ case (a, b) =>
        comment.warn(pairFormat.format(a.sliceName, b.sliceName))
      }
    }

    if (targetState.collectionInfo.configName(targetCollection) != fromState.collectionInfo.configName(fromCollection)) {
      comment.warn("Warning, configset name doesn't match for the two collections.")
    }

    // TODO: Insure target is empty?


    val actionList = for ( (fetchInto, fetchFrom) <- replicaPairs ) yield {
      val replicas = targetState.replicasFor(fetchInto.collection, fetchInto.sliceName).filterNot(_ == fetchInto)
      FetchIndex(fetchFrom.core, fetchInto.core, fetchFrom.url, confirm) +: replicas.map( r => FetchIndex(fetchInto.core, r.core, fetchInto.url, confirm))
    }
    Operation(actionList.flatten)

  }

  case class CloneCollectionOverrides(
                                       configName: Option[String] = None,
                                       maxSlicesPerNode: Option[Int] = None,
                                       replicationFactor: Option[Int] = None,
                                       createNodeSet: Option[Seq[String]] = None
                                     )
  def cloneCollection(targetClusterManager: ClusterManager, targetCollection: String,
                      fromClusterManager: ClusterManager, fromCollection: String,
                      overrides: CloneCollectionOverrides = CloneCollectionOverrides()): Operation = {

    def deleteOnExit(dir: Path): Unit = {
      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run() = recurseDelete(dir.toFile)
        def recurseDelete(file: File): Unit = {
          if (file.isDirectory) {
            file.listFiles().toList.foreach(recurseDelete)
            file.delete()
          }
          else
            file.delete()
        }
      })
    }

    val targetState = targetClusterManager.currentState
    val fromState = fromClusterManager.currentState
    val fromConfigName = overrides.configName.getOrElse(fromState.collectionInfo.configName(fromCollection))
    val fromReplicas = fromState.replicasFor(fromCollection)
    val fromSliceCount = fromReplicas.map(_.sliceName).distinct.size
    val fromMaxSlicesPerNode = overrides.maxSlicesPerNode.getOrElse(fromReplicas.groupBy(_.node).map(_._2.size).max)
    val fromReplicationFactor = overrides.replicationFactor.getOrElse(fromReplicas.groupBy(_.sliceName).map(_._2.size).max)

    val copyConfigOp: Operation =
      (targetState.configs.contains(fromConfigName), fromState.configs.contains(fromConfigName)) match {
        case (false, true) =>
          val tmpDir = Files.createTempDirectory("solrcloud_manager_config")
          deleteOnExit(tmpDir) // at least try to clean up afterwards
          Operation(Seq(
            AltClusterAction(DownloadConfig(tmpDir, fromConfigName), fromClusterManager),
            UploadConfig(tmpDir, fromConfigName)
          ))
        case (true, _) =>
          // TODO: Actually verify contents
          if (targetClusterManager != fromClusterManager)
            comment.warn(s"Config $fromConfigName already exists in the target cluster, assuming the contents are the same.")
          Operation.empty
        case (false, false) =>
          throw new OperationsException(s"Could not find a config named $fromConfigName")
      }

    val createCollectionOp = Operation(
      CreateCollection(
        targetCollection,
        fromSliceCount,
        fromConfigName,
        Some(fromMaxSlicesPerNode),
        Some(fromReplicationFactor),
        overrides.createNodeSet
      )
    )

    copyConfigOp ++ createCollectionOp
  }

  /**
   * Requests a backup of the index for a given collection. Only the leader replica for each slice will create a backup.
   *
   * In order for this to function as a reliable backup mechanism, or for the "restoreCollection" operation to work,
   * the dir must be a shared filesystem among all nodes.
   *
   * The collection and slice name will be appended to the backup dir provided. It's assumed that the
   * path separator is the same on this machine and the nodes. "keep" will be be considered per-slice.
   * So, the number of backups of each slice on a given node will be no greater than "keep".
   *
   * This does NOT back up any cluster state stored in Zookeeper, including the collection definition or config.
   * @param clusterManager
   * @param collection
   * @param dir The base directory on the node to save backups in.
   * @param keep The number of backups to keep, per slice, including this one. The new backup will be created before
   *             old ones are cleaned, so you need space for n+1 backups.
   * @param parallel Execute all backup requests without waiting to see if they finish.
   * @return An operation that backs up the given collection.
   */
  def backupCollection(clusterManager: ClusterManager, collection: String,
                       dir: String, keep: Int, parallel: Boolean): Operation = {
    val state = clusterManager.currentState
    val collectionReplicas = state.liveReplicasFor(collection)
    val backupReplicas = collectionReplicas.filter(_.leader)

    // The default Solr backup naming and retention strategy doesn't distinguish backups, so we
    // want to encode more distinguishing information in the directory structure
    def backupDir(replica: SolrReplica) = Paths.get(dir, replica.collection, replica.sliceName).toString

    Operation(backupReplicas.map(r => BackupIndex(r.core, backupDir(r), !parallel, keep)))
  }

  /**
   * Restores all cores in a given collection from the most recent backup made by this tool.
   *
   * Presumes the necessary slices are available on the necessary nodes. In practice, the only way to know this
   * for sure is if the dir is a shared filesystem across all nodes, or if you only have one node in your cluster.
   *
   * Presumes that the collection you're restoring into is mostly identical (schema, shard count, etc) to the
   * one that made the backup, and that the backup was made by this tool. Only the collection name and replication
   * factor should be different.
   * @param clusterManager
   * @param collection The name of the (existing) collection to restore into
   * @param dir The same value used for the backup command
   * @param oldCollection The name of the collection that made the backup, if different.
   * @return An operation that restores the given collection.
   */
  def restoreCollection(clusterManager: ClusterManager, collection: String,
                        dir: String, oldCollection: Option[String]): Operation = {
    val state = clusterManager.currentState
    val collectionReplicas = state.liveReplicasFor(collection)
    def backupDir(replica: SolrReplica) = {
      Paths.get(dir, oldCollection.getOrElse(replica.collection), replica.sliceName).toString
    }
    if (clusterManager.clusterVersion < SolrCloudVersion(5,5,1))
      comment.warn("WARNING: A given backup will only be restorable ONCE without manual intervention. See SOLR-8449.")
    Operation(collectionReplicas.map(r => RestoreIndex(r.core, backupDir(r))))
  }

  /**
    * Wipes fromCollection replicas from the nodes, then offers those nodes for a "fill" operation with another collection
    * @param clusterManager
    * @param fromCollection
    * @param toCollection
    * @return
    */
  def reTaskNodes(clusterManager: ClusterManager,
                  fromCollection: String,
                  toCollection: String,
                  nodes: Seq[String],
                  waitForReplication: Boolean = true): Operation = {
    val state = clusterManager.currentState
    val wipeCommands = nodes.map(node => wipeCollectionFromNode(clusterManager, fromCollection, node)).reduce(_ ++ _)
    val fillCommand = fillCluster(clusterManager, toCollection, Some(nodes.toSeq), waitForReplication)
    wipeCommands ++ fillCommand
  }


}
