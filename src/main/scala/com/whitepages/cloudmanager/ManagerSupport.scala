package com.whitepages.cloudmanager

import org.apache.log4j.{Logger, Level, LogManager}


trait ManagerSupport {
  val comment = LogManager.getLogger("console")
}

/**
 * Serves as a static reference to the logging functionality
 */
object ManagerConsoleLogging extends ManagerSupport {
  def setLevel(lvl: Level) = comment.setLevel(lvl)
}
