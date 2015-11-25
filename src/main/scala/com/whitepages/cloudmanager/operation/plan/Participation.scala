package com.whitepages.cloudmanager.operation.plan


import scala.annotation.tailrec


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

  def assignSlots(availableNodes: Set[String], availableSlots: Int) =
    Participation.assignSlots(availableNodes, availableSlots, this)
}

object Participation {
  /**
    * Given some number of desired additional assignments, returns the specific assignments
    * that best increase the replication factor.
    * @param availableNodes A list of nodes available for assignments
    * @param availableSlots The number of desired additional assignments
    * @param participation The current participation state
    * @param assignments Any assignments so far
    * @return A sequence of assignments
    */
  @tailrec
  def assignSlots(
                         availableNodes: Set[String],
                         availableSlots: Int,
                         participation: Participation,
                         assignments: Seq[Assignment] = Nil): Seq[Assignment] = {
    if (availableSlots <= 0) {
      assignments
    }
    else {
      // the slice with the fewest replicas
      val minSlice = participation.nodesPerSlice.minBy(_._2)._1
      val nodesWithoutSlice = availableNodes -- participation.sliceParticipants(minSlice).map(_.node)
      // the node with the fewest replicas that doesn't have the slice with the fewest replicas
      val minNode = nodesWithoutSlice.minBy( participation.sliceCount )
      val assignment = Assignment(minNode, minSlice)
      assignSlots(
        availableNodes,
        availableSlots - 1,
        participation + assignment,
        assignments :+ assignment
      )
    }
  }


}
