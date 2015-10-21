package com.whitepages.cloudmanager.client

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.solr.common.util.NamedList

import scala.concurrent.duration._

trait SolrResponseHelper {
  implicit val rsp: NamedList[AnyRef]

  // solr response objects are annoying.
  def walk(directions: String*): Option[String] = walk(directions.toList)
  def walk(directions: List[String])(implicit node: NamedList[AnyRef]): Option[String] = {
    directions.length match {
      case 0 => throw new RuntimeException("Recursed one too many times")
      case 1 => {
        val destination = node.get(directions.head)
        if (destination == null)
          None
        else
          Some(destination.toString)
      }
      case _ => {
        val step = node.get(directions.head)
        if (step == null)
          None
        else
          walk(directions.tail)(step.asInstanceOf[NamedList[AnyRef]])
      }
    }
  }
  def get(key: String) = rsp.get(key)

  lazy val status = walk("responseHeader", "status").getOrElse("-100")
}


case class GenericSolrResponse(rsp: NamedList[AnyRef]) extends SolrResponseHelper

case class ReplicationStateResponse(rsp: NamedList[AnyRef]) extends SolrResponseHelper {
  private val backupDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ROOT)

  lazy val replicating = walk("details", "slave", "isReplicating")
  lazy val replicationTimeRemaining =
    walk("details", "slave", "timeRemaining").map(_.replace("s", "").toInt.seconds)
  lazy val generation = walk("details", "generation")
  lazy val indexVersion = walk("details", "indexVersion")
  lazy val lastBackupSucceeded = walk("details", "backup", "status").map(_.toLowerCase == "success")
  lazy val lastBackup = walk("details", "backup", "snapshotCompletedAt").map(backupDateFormat.parse)
}

case class LukeStateResponse(rsp: NamedList[AnyRef]) extends SolrResponseHelper {
  lazy val numDocs = walk("index", "numDocs").map(_.toInt)
  lazy val version = walk("index", "version")
  lazy val current = walk("index", "current").map(s => if (s == "true") true else false)
}

case class SystemStateResponse(rsp: NamedList[AnyRef]) extends SolrResponseHelper {
  lazy val solrVersion = walk("lucene", "solr-spec-version").map(SolrCloudVersion(_)).getOrElse(SolrCloudVersion.unknown)
}

object SolrCloudVersion {
  def parseVersion(version: String): SolrCloudVersion = {
    val cleanVersion = version.trim.replaceAll("""\s""", "").replaceAll("-.*$", "")
    val versions = cleanVersion.split('.')
    val major = if (versions.length > 0) versions(0).toInt else 0
    val minor = if (versions.length > 1) versions(1).toInt else 0
    val patch = if (versions.length > 2) versions(2).toInt else 0
    SolrCloudVersion(major, minor, patch)
  }
  def apply(version: String): SolrCloudVersion = parseVersion(version)
  val unknown = SolrCloudVersion(0,0,0)
}
case class SolrCloudVersion(major: Int, minor: Int, patch: Int = 0) extends Ordered[SolrCloudVersion] {
  override def compare(that: SolrCloudVersion): Int = {
    if (major != that.major)      major.compareTo(that.major)
    else if (minor != that.minor) minor.compareTo(that.minor)
    else if (patch != that.patch) patch.compareTo(that.patch)
    else 0
  }
  override val toString = List(major, minor, patch).mkString(".")
}
