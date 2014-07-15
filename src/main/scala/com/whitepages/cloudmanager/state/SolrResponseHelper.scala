package com.whitepages.cloudmanager.state

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

  lazy val replicating = walk("details", "slave", "isReplicating")
  lazy val replicationTimeRemaining =
    walk("details", "slave", "timeRemaining").map(_.replace("s", "").toInt.seconds)
  lazy val generation = walk("details", "generation")
  lazy val indexVersion = walk("details", "indexVersion")
}
