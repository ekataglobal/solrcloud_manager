package com.whitepages.cloudmanager

import org.apache.log4j.{Logger, Level, LogManager}
import scala.concurrent.duration.FiniteDuration


trait ManagerSupport {
  val comment = LogManager.getLogger("console")

  // Like sleep, but returns true for inserting delay into logical "and" chains
  def delay(ms: FiniteDuration): Boolean = delay(ms.toMillis)
  def delay(ms: Long): Boolean = { Thread.sleep(ms); true }

}

/**
 * Serves as a static reference to the logging functionality for those classes that don't mix in ManagerSupport
 */
object ManagerConsoleLogging extends ManagerSupport {
  def setLevel(lvl: Level) = comment.setLevel(lvl)
}
