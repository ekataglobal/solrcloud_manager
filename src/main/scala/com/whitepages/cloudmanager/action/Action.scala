package com.whitepages.cloudmanager.action

import org.apache.solr.client.solrj.impl.CloudSolrServer
import com.whitepages.cloudmanager.state._
import org.apache.solr.client.solrj.request.QueryRequest
import scala.util.{Random, Failure, Success, Try}
import org.apache.solr.common.util.NamedList
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.solr.common.params.CollectionParams.CollectionAction
import scala.concurrent.duration._
import scala.annotation.tailrec
import org.apache.solr.client.solrj.SolrServer
import com.whitepages.cloudmanager.state.SolrState
import com.whitepages.cloudmanager.state.GenericSolrResponse
import scala.util.Failure
import scala.Some
import com.whitepages.cloudmanager.state.ClusterManager
import com.whitepages.cloudmanager.state.ReplicationStateResponse
import scala.util.Success

case class StateCondition(name: String, check: (SolrState) => Boolean)

trait Action {
  
  val name: String = this.getClass.getSimpleName

  val preConditions: List[StateCondition]
  def canPerform(state: SolrState) = checkAndReport(preConditions, state)

  def execute(clusterManager: ClusterManager): Boolean

  def perform(clusterManager: ClusterManager): Boolean = {
    if ( canPerform(clusterManager.currentState) ) {
      var actionSuccess = true

      if (execute(clusterManager)) {
        println(s"Applied $this")
      }
      else {
        println(s"Could not apply $this")
        actionSuccess = false
      }

      if (actionSuccess) {
        if (verify(clusterManager.currentState)) {
          println(s"Verified $this")
        }
        else {
          println(s"Failed to verify $this")
          actionSuccess = false
        }
      }

      actionSuccess
    }
    else false
  }

  val postConditions: List[StateCondition]
  def verify(state: SolrState) = checkAndReport(postConditions, state)

  private def check(conditions: List[StateCondition], state: SolrState): Boolean = conditions.forall(_.check(state))
  private def checkAndReport(conditions: List[StateCondition], state: SolrState): Boolean = {
    conditions.forall {
      condition => condition.check(state) match {
        case false => println(s"Failed: ${condition.name}"); false
        case true  => println(s"Success: ${condition.name}"); true
      }
    }
  }

  object SolrRequestHelpers {

    val asyncPause = 10 seconds

    def getSolrResponse(client: SolrServer, params: ModifiableSolrParams, path: String = "/admin/collections") = {
      val req = new QueryRequest(params)
      req.setPath(path)
      println(s"REQUEST: ${req.getPath} ${req.getParams}")
      Try(client.request(req)).map(GenericSolrResponse)
    }

    def submitRequest(client: SolrServer, params: ModifiableSolrParams, path: String = "/admin/collections"): Boolean = {
      val response = getSolrResponse(client, params, path)
      response match {
        case Success(r) => {
          println(s"RESPONSE: $response")
          checkStatus(r)
        }
        case Failure(e) => println(s"Request failed: $e"); false
      }
    }

    def checkStatus(rsp: SolrResponseHelper) = {
      rsp.status == "0" && rsp.walk("failure") == None
    }
    def checkAsyncJobStatus(rsp: SolrResponseHelper) = {
      rsp.walk("status", "state") != Some("running")
    }

    def submitAsyncRequest(client: CloudSolrServer, params: ModifiableSolrParams): Boolean = {
      val requestId = Random.nextInt(1000000).toString
      params.set("async", requestId)
      val response = getSolrResponse(client, params)
      val success = response match {
        case Success(r) => checkStatus(r)
        case Failure(e) => println(s"Request failed: $e"); false
      }
      if (success) {
        monitorAsyncRequest(client, requestId)
      } else false

    }

    @tailrec
    def monitorAsyncRequest(client: CloudSolrServer, requestId: String): Boolean = {
      val checkParams = new ModifiableSolrParams
      checkParams.set("action", CollectionAction.REQUESTSTATUS.toString)
      checkParams.set("requestid", requestId)
      val checkResponse = getSolrResponse(client, checkParams)
      checkResponse match {
        case Success(r) if checkStatus(r) => {
          println(s"RESPONSE: $r")
          if (checkAsyncJobStatus(r)) {
            true
          }
          else {
            println(s"Waiting for async request $requestId")
            Thread.sleep(asyncPause.toMillis)
            monitorAsyncRequest(client, requestId)
          }
        }
        case Success(r) if !checkStatus(r) => {
          println(s"Unexpected response checking async request $requestId: $r")
          false
        }
        case Failure(e) => {
          println(s"Error checking async job status. ID $requestId, $e")
          false
        }
      }
    }
  }

  object ReplicationHelpers {
    def checkFetchIndexResponse(rsp: SolrResponseHelper): Boolean = {
      rsp.status == "0" && rsp.walk("status") == Some("OK")
    }
    def checkReplicationStatusResponse(rsp: SolrResponseHelper): Boolean = {
      rsp.status == "0"
    }
    def checkRemainingReplicationTime(rsp: ReplicationStateResponse): Int = {
      (rsp.replicating, rsp.replicationTimeRemaining) match {
        case (None, _) => 1 // the appropriate node hasn't shown up yet
        case (Some("false"), _) => 0 // all done
        case (Some(_), Some(remaining)) => remaining.toSeconds.toInt
        case (Some(_), None) => 10 // we're replicating, but for some reason we can't tell how long is remaining
      }
    }

    @tailrec
    def waitForReplication(client: SolrServer, node: String):Boolean = {

      val params = new ModifiableSolrParams
      params.set("command", "details")

      val checkResponse = SolrRequestHelpers.getSolrResponse(client, params, s"/$node/replication")
      checkResponse match {
        case Success(r) if checkReplicationStatusResponse(r) => {
          val rsp = ReplicationStateResponse(r.rsp)
          println(s"RESPONSE: $r")
          val remaining = checkRemainingReplicationTime(rsp).seconds
          if (remaining == 0.seconds) {
            true
          }
          else {
            println(s"Waiting for replication, expected duration: ${remaining.toSeconds} seconds")
            Thread.sleep(Math.min(remaining.toMillis / 2, 1000))
            waitForReplication(client, node)
          }
        }
        case Success(r) if !checkReplicationStatusResponse(r) => {
          println(s"Unexpected response checking replication status: $r")
          false
        }
        case Failure(e) => {
          println(s"Error checking replication status, $e")
          false
        }
      }

    }
  }

}