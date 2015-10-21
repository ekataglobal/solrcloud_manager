package com.whitepages.cloudmanager.action

import java.nio.file.Path
import java.util.Date

import com.whitepages.cloudmanager.client.{SolrRequestHelpers, ReplicationHelpers}
import com.whitepages.cloudmanager.state.ClusterManager
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.common.params.ModifiableSolrParams
import scala.concurrent.duration._

/**
 * Copies the snapshot of a core into another directory.
 * @param core The core name to trigger a backup for
 * @param backupDir The directory on the node to save the backup into (absolute path suggested)
 * @param numberToKeep Number of backups to keep, including this one. Note that the new backup is created
 *                     before the old ones are cleaned, so you need space for n+1 backups briefly.
 */
case class Backup(core: String, backupDir: String, checkStatus: Boolean = true, numberToKeep: Int = 2) extends Action {

  override def execute(clusterManager: ClusterManager): Boolean = {

    // go to the node directly when using the replication handler
    val targetReplica = clusterManager.currentState.allReplicas.filter( (r) => r.core == core ).head
    val url = targetReplica.url
    val client = new HttpSolrClient(url)

    val params = new ModifiableSolrParams
    params.set("command", "backup")
    params.set("location", backupDir)
    params.set("numberToKeep", numberToKeep)

    // replication timestamp is used for the success test and only has second-resolution, so back-date
    // a bit to be sure we notice if it took less than a second.
    val now = new Date(System.currentTimeMillis() - 1.second.toMillis)

    SolrRequestHelpers.submitRequest(client, params, s"/$core/replication") &&
      (if (checkStatus) ReplicationHelpers.waitForBackup(client, core, now) else true)
  }

  override val preConditions = List(
    StateCondition("core exists", Conditions.coreNameExists(core))
  )
  override val postConditions: List[StateCondition] = Nil
}
