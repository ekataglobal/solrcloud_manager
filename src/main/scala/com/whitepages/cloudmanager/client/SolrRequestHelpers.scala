package com.whitepages.cloudmanager.client

import java.util.Date

import com.whitepages.cloudmanager.ManagerSupport
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.QueryRequest
import org.apache.solr.common.params.CollectionParams.CollectionAction
import org.apache.solr.common.params.ModifiableSolrParams
import scala.concurrent.duration._
import scala.annotation.tailrec
import scala.util.{Random, Failure, Success, Try}

object SolrRequestHelpers extends ManagerSupport {

  val asyncPause = 10 seconds

  def getSolrResponse(client: SolrClient, params: ModifiableSolrParams, path: String = "/admin/collections") = {
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

  def submitRequest(client: SolrClient, params: ModifiableSolrParams, path: String = "/admin/collections"): Boolean = {
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

  def submitAsyncRequest(client: CloudSolrClient, params: ModifiableSolrParams): Boolean = {
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
  def monitorAsyncRequest(client: CloudSolrClient, requestId: String): Boolean = {
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
          delay(asyncPause)
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

object ReplicationHandlerHelpers extends ManagerSupport {

  private val detailsReq = new ModifiableSolrParams
  detailsReq.set("command", "details")
  private val restoreStatusReq = new ModifiableSolrParams
  restoreStatusReq.set("command", "restorestatus")

  def getReplicationDetails(client: SolrClient, core: String): Try[ReplicationStateResponse] = {
    val checkResponse = SolrRequestHelpers.getSolrResponse(client, detailsReq, s"/$core/replication")
    checkResponse.map(r => ReplicationStateResponse(r.rsp))
  }

  def getRestoreStatus(client: SolrClient, core: String): Try[RestoreStateResponse] = {
    val restoreResponse = SolrRequestHelpers.getSolrResponse(client, restoreStatusReq, s"/$core/replication")
    restoreResponse.map(r => RestoreStateResponse(r.rsp))
  }

  def checkFetchIndexResponse(rsp: SolrResponseHelper): Boolean = {
    rsp.status == "0" && rsp.walk("status") == Some("OK")
  }
  def checkStatusResponse(rsp: SolrResponseHelper): Boolean = {
    rsp.status == "0"
  }

  def checkRemainingReplicationTime(rsp: ReplicationStateResponse): Int = {
    (rsp.replicating, rsp.replicationTimeRemaining) match {
      case (None, _) => 1 // the appropriate node hasn't shown up yet
      case (Some("false"), _) => 0 // all done
      case (Some(_), Some(remaining)) if remaining == 0.seconds =>
        // the timeRemaining estimate initializes to 0 seconds, so ignore zeros and give it time to build a real estimate
        5
      case (Some(_), Some(remaining)) => remaining.toSeconds.toInt
      case (Some(_), None) => 1 // we're replicating, but for some reason we can't tell how long is remaining
    }
  }

  @tailrec
  def waitForReplication(client: SolrClient, core: String):Boolean = {

    val checkResponse = getReplicationDetails(client, core)
    checkResponse match {
      case Success(r) if checkStatusResponse(r) => {
        val remaining = checkRemainingReplicationTime(r).seconds
        if (remaining == 0.seconds) {
          true
        }
        else {
          comment.info(s"Waiting for replication, expected duration: ${remaining.toSeconds} seconds")
          val delayMillis = if (remaining > 600.seconds) 300.seconds.toMillis else remaining.toMillis / 2
          delay(Math.max(delayMillis, 1000))
          waitForReplication(client, core)
        }
      }
      case Success(r) if !checkStatusResponse(r) => {
        comment.warn(s"Unexpected response checking replication status: $r")
        false
      }
      case Failure(e) => {
        comment.warn(s"Error checking replication status, $e")
        false
      }
    }
  }

  @tailrec
  def waitForBackup(client: SolrClient, core: String, since: Date): Boolean = {
    val checkResponse = getReplicationDetails(client, core)
    checkResponse match {
      case Success(r) if checkStatusResponse(r) => {
        (r.lastBackup, r.lastBackupSucceeded) match {
          case (Some(when), Some(succeeded)) if when.after(since) =>
            // the last backup date doesn't get updated until the backup finishes
            succeeded
          case _ =>
            comment.info(s"Waiting for backup request of $core to complete")
            delay(3.seconds)
            waitForBackup(client, core, since)
        }
      }
      case Success(r) if !checkStatusResponse(r) => {
        comment.warn(s"Unexpected response checking replication status: $r")
        false
      }
      case Failure(e) => {
        comment.warn(s"Error checking replication status, $e")
        false
      }
    }

  }

  @tailrec
  def waitForRestore(client: SolrClient, core: String): Boolean = {
    val checkResponse = getRestoreStatus(client, core)
    checkResponse match {
      case Success(r) if checkStatusResponse(r) =>
        if (r.restoreSuccess) true
        else if (r.restoreFailure) false
        else {
          comment.info(s"Waiting for restore request of $core to complete")
          delay(3.seconds)
          waitForRestore(client, core)
        }
      case Success(r) if !checkStatusResponse(r) => {
        comment.warn(s"Unexpected response checking restore status: $r")
        false
      }
      case Failure(e) => {
        comment.warn(s"Error checking restore status, $e")
        false
      }
    }
  }
}

