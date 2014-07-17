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
import com.whitepages.cloudmanager.ManagerSupport

case class StateCondition(name: String, check: (SolrState) => Boolean)

trait Action extends ManagerSupport {
  
  val name: String = this.getClass.getSimpleName

  val preConditions: List[StateCondition]
  def canPerform(state: SolrState) = checkAndReport(preConditions, state)

  def execute(clusterManager: ClusterManager): Boolean

  def perform(clusterManager: ClusterManager): Boolean = {
    if ( canPerform(clusterManager.currentState) ) {
      var actionSuccess = true

      if (execute(clusterManager)) {
        comment.info(s"Applied $this")
      }
      else {
        comment.info(s"Could not apply $this")
        actionSuccess = false
      }

      if (actionSuccess) {
        if (verify(clusterManager.currentState)) {
          comment.info(s"Verified $this")
        }
        else {
          comment.info(s"Failed to verify $this")
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
        case false => comment.warn(s"Check Failed: ${condition.name}"); false
        case true  => comment.info(s"Check Succeeded: ${condition.name}"); true
      }
    }
  }

  object SolrRequestHelpers {

    val asyncPause = 10 seconds

    def getSolrResponse(client: SolrServer, params: ModifiableSolrParams, path: String = "/admin/collections") = {
      val req = new QueryRequest(params)
      req.setPath(path)
      comment.debug(s"REQUEST: ${req.getPath} ${req.getParams}")
      val response = Try(client.request(req)).map(GenericSolrResponse)
      response match {
        case Success(r) =>
          comment.debug(s"RESPONSE: $r")
        case Failure(e) =>
          comment.debug(s"RESPONSE (exception): $e")
      }
      response
    }

    def submitRequest(client: SolrServer, params: ModifiableSolrParams, path: String = "/admin/collections"): Boolean = {
      val response = getSolrResponse(client, params, path)
      response match {
        case Success(r) =>
          checkStatus(r)
        case Failure(e) =>
          comment.warn(s"Request failed: $e")
          false
      }
    }

    def checkStatus(rsp: SolrResponseHelper) = {
      rsp.status == "0" && rsp.walk("failure") == None
    }
    private val waitingStates = Set[Option[String]]( Some("submitted"), Some("running") )
    def checkAsyncJobStatus(rsp: SolrResponseHelper) = {
      !waitingStates.contains(rsp.walk("status", "state"))
    }

    def submitAsyncRequest(client: CloudSolrServer, params: ModifiableSolrParams): Boolean = {
      val requestId = Random.nextInt(100000000).toString
      params.set("async", requestId)
      val response = getSolrResponse(client, params)
      val success = response match {
        case Success(r) => checkStatus(r)
        case Failure(e) => comment.warn(s"Request failed: $e"); false
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
          comment.debug(s"RESPONSE: $r")
          if (checkAsyncJobStatus(r)) {
            true
          }
          else {
            comment.info(s"Waiting for async request $requestId")
            Thread.sleep(asyncPause.toMillis)
            monitorAsyncRequest(client, requestId)
          }
        }
        case Success(r) if !checkStatus(r) => {
          comment.warn(s"Unexpected response checking async request $requestId: $r")
          false
        }
        case Failure(e) => {
          comment.warn(s"Error checking async job status. ID $requestId, $e")
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
        case (Some(_), Some(remaining)) =>
          // the timeRemaining estimate initializes to 0 seconds, so ignore zeros and give it time to build a real estimate
          Math.max(remaining.toSeconds.toInt, 10)
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
          val remaining = checkRemainingReplicationTime(rsp).seconds
          if (remaining == 0.seconds) {
            true
          }
          else {
            comment.info(s"Waiting for replication, expected duration: ${remaining.toSeconds} seconds")
            Thread.sleep(Math.max(remaining.toMillis / 2, 1000))
            waitForReplication(client, node)
          }
        }
        case Success(r) if !checkReplicationStatusResponse(r) => {
          comment.warn(s"Unexpected response checking replication status: $r")
          false
        }
        case Failure(e) => {
          comment.warn(s"Error checking replication status, $e")
          false
        }
      }

    }
  }

}
