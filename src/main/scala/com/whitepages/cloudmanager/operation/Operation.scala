package com.whitepages.cloudmanager.operation


import org.apache.solr.client.solrj.impl.{CloudSolrClient}
import com.whitepages.cloudmanager.action.Action
import com.whitepages.cloudmanager.state.ClusterManager
import java.util.Calendar
import com.whitepages.cloudmanager.ManagerSupport
import scala.collection.JavaConverters._


object Operation {
  def empty = Operation(Seq())
}
case class Operation(actions: Seq[Action]) extends ManagerSupport {
  def this(actions: java.util.List[Action]) = this(actions.asScala)

  private val calendar = Calendar.getInstance()

  def prettyPrint = {
    "Operation: \n" + actions.map("\t" + _.toString).mkString("\n")
  }

  def execute(client: CloudSolrClient): Boolean = execute(ClusterManager(client))
  def execute(clusterManager: ClusterManager): Boolean = {
    if (actions.isEmpty) {
      true
    } else {
      comment.debug(calendar.getTime + " Beginning " + this)
      val success = actions.foldLeft(true)((goodSoFar, action) => {
        if (goodSoFar) {
          comment.debug(s"Starting to apply $action")
          val actionSuccess = if (action.perform(clusterManager)) {
            comment.debug(s"Finished applying $action")
            true
          }
          else {
            comment.warn(s"Could not apply $action")
            false
          }
          actionSuccess
        }
        else goodSoFar
      })
      comment.debug(calendar.getTime + " Finished " + this)
      success
    }
  }


  // TODO: More collection-y api? These are all I really needed so far.
  def isEmpty = actions.isEmpty
  def nonEmpty = actions.nonEmpty
  def ++(that: Operation) = Operation(this.actions ++ that.actions)
}


