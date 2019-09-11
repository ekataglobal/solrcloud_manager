package com.whitepages.cloudmanager.operation.plan


import com.whitepages.cloudmanager.state.SolrReplica

import scala.annotation.tailrec
import scala.collection.immutable


case class Assignment(node: String, slice: String)
case class Participation(slices: Set[String], assignments: Seq[Assignment]) {
  lazy val nodeParticipants: Map[String, Seq[Assignment]] = assignments.groupBy(_.node).withDefaultValue(Seq())
  lazy val sliceParticipants: Map[String, Seq[Assignment]] = {
    val absentSlices: Map[String, Seq[Assignment]] = slices.map( (_, Seq()) ).toMap
    (absentSlices ++ assignments.groupBy(_.slice)).withDefaultValue(Seq())
  }

  private def participationCounts(p: Map[String, Seq[Assignment]]): Map[String, Int] = {
    p.map { case (node, nodeAssignments) => (node, nodeAssignments.size) }.withDefaultValue(0)
  }

  lazy val slicesPerNode: Map[String, Int] = participationCounts(nodeParticipants)
  lazy val nodesPerSlice: Map[String, Int] = participationCounts(sliceParticipants)
  def sliceCount(node: String) = slicesPerNode(node)
  def nodeCount(slice: String) = nodesPerSlice(slice)

  def +(newAssignment: Assignment) = Participation(slices, assignments :+ newAssignment)

  def assignSlots(availableNodes: Set[String], availableSlots: Int, openSpace: Map[String, Int] = Map()) =
    Participation.assignSlots(availableNodes, availableSlots, this, openSpace)
}

object Participation {

  private def existsInNode(node: String, slice: String, participation: Participation): Boolean = {
    val nodesWithSlice = participation.sliceParticipants(slice).map(_.node)
    nodesWithSlice.contains(node)
  }

  private def recursiveBacktracking(availableNodes: Set[String],
                            nodeSpace: Map[String, Int],
                            numSlots: Int,
                            participation: Participation,
                            assignments: Seq[Assignment]): Option[Seq[Assignment]] = {
    if (numSlots == 0 || nodeSpace.isEmpty)
      Some(assignments)
    else {
      // Pick out the least-common slice
      val minSlice: String = participation.nodesPerSlice.toList.minBy(_._2)._1

      // Sort nodes in order of least-populated-first
      val bestNodes: immutable.List[(String, Int)] = nodeSpace.toList.sortBy(-_._2)

      // Loop through nodes. Assign the slice to the first one that it doesn't already exist in.
      // Implemented as recursive backtracking in case a certain placement fails; however, in
      // all testing done there's no backtracking required. Don't have a proof that it
      // never backtracks, though
      var toRet: Option[Seq[Assignment]] = None
      for (bestNode <- bestNodes) {
        val asn = Assignment(bestNode._1, minSlice)
        if (toRet.isEmpty && !existsInNode(bestNode._1, minSlice, participation) && bestNode._2 > 0) {
          // UNCOMMENT TO: Visualize the order in which we considered assignments
          // println(" "*assignments.length+": "+asn)
          val res = recursiveBacktracking(availableNodes,
            nodeSpace + (bestNode._1 -> (bestNode._2 - 1)),
            numSlots - 1,
            participation + asn,
            assignments :+ asn)

          toRet = res
        }
      }
      toRet
    }
  }


  /**
    * Given some number of desired additional assignments, returns the specific assignments
    * that best increase the replication factor.
    * @param availableNodes A list of nodes available for assignments
    * @param availableSlots The number of desired additional assignments
    * @param participation The current participation state
    * @param assignments Any assignments so far
    * @return A sequence of assignments
    */

  def assignSlots(
                         availableNodes: Set[String],
                         availableSlots: Int,
                         participation: Participation,
                         availableSpace: Map[String, Int] = Map(),
                         assignments: Seq[Assignment] = Nil): Seq[Assignment] = {

    val attempt = recursiveBacktracking(availableNodes, availableSpace, availableSlots, participation, assignments)
    if (attempt.isEmpty) {
      println("ATTEMPT TO PUT NODES FAILED")
      throw new IllegalStateException("Couldn't fit nodes on!");
    }
    attempt.get
  }

  def fromReplicas(slices: Set[String], replicas: Seq[SolrReplica]): Participation =
    Participation(slices, replicas.map((replica) => Assignment(replica.node, replica.sliceName)))

}
