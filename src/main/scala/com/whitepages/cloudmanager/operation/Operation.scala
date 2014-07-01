package com.whitepages.cloudmanager.operation

import org.apache.solr.client.solrj.impl.CloudSolrServer
import com.whitepages.cloudmanager.action.Action
import com.whitepages.cloudmanager.state.ClusterManager
import java.util.Calendar


object Operation {
  def empty = Operation(Seq())
}
case class Operation(actions: Seq[Action]) {

  private val calendar = Calendar.getInstance()

  def prettyPrint = {
    "Operation: \n" + actions.map("\t" + _.toString).mkString("\n")
  }

  def execute(client: CloudSolrServer): Boolean = execute(ClusterManager(client))
  def execute(clusterManager: ClusterManager): Boolean = {
    if (actions.isEmpty) {
      true
    } else {
      println(calendar.getTime + " Beginning " + this)
      val success = actions.foldLeft(true)((goodSoFar, action) => {
        if (goodSoFar) {
          println(s"Starting to apply $action")
          val actionSuccess = if (action.perform(clusterManager)) {
            println(s"Finished applying $action")
            true
          }
          else {
            println(s"Could not apply $action")
            false
          }
          actionSuccess
        }
        else goodSoFar
      })
      println(calendar.getTime + " Finished " + this)
      success
    }
  }


  // TODO: More collection-y api? These are all I really needed so far.
  def isEmpty = actions.isEmpty
  def nonEmpty = actions.nonEmpty
  def ++(that: Operation) = Operation(this.actions ++ that.actions)
}


