package com.whitepages.cloudmanager.operation

import com.whitepages.cloudmanager.action.{FetchIndex, DeleteReplica, AddReplica}
import org.apache.solr.client.solrj.impl.CloudSolrServer
import com.whitepages.cloudmanager.state.ClusterManager
import scala.annotation.tailrec

object Operations {

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
    println(s"Populate Operation Found: Available Nodes - ${nodesWithoutCollection.size}, Replication factor - $replicationFactor, nodesPerSet - $nodesPerSet")
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
   * @return The corresponding Operation
   */
  def fillCluster(clusterManager: ClusterManager, collection: String): Operation = {
    val state = clusterManager.currentState

    case class Assignment(node: String, slice: String)
    case class Participation(assignments: Seq[Assignment]) {
      lazy val nodeParticipants = assignments.groupBy(_.node).withDefaultValue(Seq())
      lazy val sliceParticipants = assignments.groupBy(_.slice).withDefaultValue(Seq())

      private def participationCounts(p: Map[String, Seq[Assignment]]) =
        p.map{ case (node, nodeAssignments) => (node, nodeAssignments.size) }.withDefaultValue(0)

      lazy val slicesPerNode = participationCounts(nodeParticipants)
      lazy val nodesPerSlice = participationCounts(sliceParticipants)
      def sliceCount(node: String) = slicesPerNode(node)
      def nodeCount(slice: String) = nodesPerSlice(slice)

      def +(newAssignment: Assignment) = Participation(assignments :+ newAssignment)
    }

    assert(state.collections.contains(collection), s"Could find collection $collection")
    val currentReplicas = state.replicasFor(collection)
    val participation = Participation(currentReplicas.map((replica) => Assignment(replica.node, replica.sliceName)))

    // use the node with the most slices as a limiter for how many slices to allow per node
    val maxSlicesPerNode = participation.slicesPerNode.maxBy(_._2)._2
    val currentSlots = currentReplicas.size
    val availableSlots = maxSlicesPerNode * state.liveNodes.size - currentSlots

    @tailrec
    def assignSlot(actions: Seq[AddReplica], participation: Participation, availableSlots: Int): Seq[AddReplica] = {
      if (availableSlots == 0) {
        actions
      }
      else {
        // the slice with the fewest replicas
        val minSlice = participation.nodesPerSlice.minBy(_._2)._1
        println(state.liveNodes.size, minSlice, participation.sliceParticipants(minSlice))
        val nodesWithoutSlice = state.liveNodes -- participation.sliceParticipants(minSlice).map(_.node)
        // the node with the fewest replicas that doesn't have the slice with the fewest replicas
        val minNode = nodesWithoutSlice.minBy( participation.sliceCount )

        assignSlot(
          actions :+ AddReplica(collection, minSlice, minNode),
          participation + Assignment(minNode, minSlice),
          availableSlots - 1)
      }
    }

    println(maxSlicesPerNode, participation, currentSlots, availableSlots)
    Operation(assignSlot(Seq(), participation, availableSlots))
  }

  /**
   * Removes any inactive replicas for a given collection.
   * A replica could be "inactive" because it's in a bad state, because the hosting node is down, or because the relevant slice
   * A node need not be up for the replica to be removed.
   * Note that SOLR-6072 means any files on the relevent node are NOT deleted.
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
  def wipeNode(clusterManager: ClusterManager, node: String): Operation = {
    val state = clusterManager.currentState
    val replicasOnNode = state.allReplicas.filter(_.node == node)
    Operation(replicasOnNode.map( (replica) => DeleteReplica(replica.collection, replica.sliceName, replica.node)) )
  }

  /**
   * Delete all replicas for a given collection from the given node
   * @param clusterManager
   * @param collection
   * @param node
   * @return The corresponding Operation
   */
  def wipeCollectionFromNode(clusterManager: ClusterManager, collection: String, node: String): Operation = {
    val state = clusterManager.currentState
    val replicasOnNode = state.replicasFor(collection).filter(_.node == node)
    Operation(replicasOnNode.map( (replica) => DeleteReplica(replica.collection, replica.sliceName, replica.node)) )
  }

  def deployFromAnotherCluster(clusterManager: ClusterManager, collection: String, deployFrom: String): Operation = {
    def firstCore(coreName: String) = coreName.replaceAll("""replica\d""", "replica1")
    def node2host(nodeName: String) = nodeName.substring(0, nodeName.indexOf('_'))

    val state = clusterManager.currentState
    val replicaGroup = state.replicasFor(collection).groupBy(_.sliceName).values.toList.sortBy(_.head.core)
    val operations = for { replicas <- replicaGroup } yield {
      val (leader, copies) = replicas.partition(_.leader)
      Operation(
        leader.flatMap( (r) =>
          FetchIndex(firstCore(r.core), r.core, deployFrom) +: copies.map( (c) => FetchIndex(r.core, c.core, node2host(r.node)))
        )
      )
    }
    operations.fold(Operation.empty)(_ ++ _)
  }


}