package com.whitepages.cloudmanager.action

import org.apache.solr.client.solrj.impl.CloudSolrServer
import com.whitepages.cloudmanager.state.{ClusterManager, SolrState}
import org.apache.solr.client.solrj.request.QueryRequest
import scala.util.{Random, Failure, Success, Try}
import org.apache.solr.common.util.NamedList
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.solr.common.params.CollectionParams.CollectionAction
import scala.concurrent.duration._
import scala.annotation.tailrec

case class StateCondition(name: String, check: (SolrState) => Boolean)

trait Action {
  
  val name: String = this.getClass.getSimpleName

  val preConditions: List[StateCondition]
  def canPerform(state: SolrState) = checkAndReport(preConditions, state)

  def execute(clusterManager: ClusterManager): Boolean

  def perform(clusterManager: ClusterManager): Boolean = {
    if ( canPerform(clusterManager.currentState)) {
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

    def getSolrResponse(client: CloudSolrServer, params: ModifiableSolrParams) = {
      val req = new QueryRequest(params)
      req.setPath("/admin/collections")
      Try(client.request(req))
    }

    def submitRequest(client: CloudSolrServer, params: ModifiableSolrParams): Boolean = {
      val response = getSolrResponse(client, params)
      response match {
        case Success(r) => {
          println(s"RESPONSE: $response")
          checkStatus(r)
        }
        case Failure(e) => println(s"Request failed: $e"); false
      }
    }

    def checkStatus(rsp: NamedList[AnyRef]) = {
      rsp.get("responseHeader").asInstanceOf[NamedList[AnyRef]].get("status").toString == "0" &&
        rsp.get("failure") == null  // some collections api requests indicate status == 0 but add this node to indicate failure
    }
    def checkAsyncJobStatus(rsp: NamedList[AnyRef]) = rsp.get("status").asInstanceOf[NamedList[AnyRef]].get("state").toString != "running"

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

}
