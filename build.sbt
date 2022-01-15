import com.typesafe.sbt.SbtStartScript

import AssemblyKeys._

name := "solrcloud_manager"

organization := "com.whitepages"

version := "1.7.0"

scalaVersion := "2.11.8"

fork in Test := true

javaOptions += "-ea"

SbtStartScript.startScriptForJarSettings

assemblySettings

resolvers += "Restlet Repository" at "http://maven.restlet.org"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies ++= Seq(
  "org.apache.solr"      %  "solr-test-framework" % "5.5.0" % "test",     // must precede solrj in the classpath
  "org.scalatest"        %% "scalatest"           % "2.2.4" % "test",
  "com.novocode"         %  "junit-interface"     % "0.11"  % "test",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.1",
  "org.apache.logging.log4j" % "log4j-api"        % "2.17.1",
  "org.apache.logging.log4j" % "log4j-1.2-api"    % "2.17.1",
  "org.apache.solr"      %  "solr-solrj"          % "5.5.0",
  "com.github.scopt"     %% "scopt"               % "3.3.0"
)

mergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith "Log4j2Plugins.dat" => MergeStrategy.discard
  case x =>
    val oldStrategy = (mergeStrategy in assembly).value
    oldStrategy(x)
}
