import com.typesafe.sbt.SbtStartScript

import AssemblyKeys._

name := "solrcloud_manager"

organization := "com.whitepages"

version := "1.3.0"

scalaVersion := "2.11.4"

fork in Test := true

javaOptions += "-ea"

SbtStartScript.startScriptForJarSettings

assemblySettings

resolvers += "Restlet Repository" at "http://maven.restlet.org"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies ++= Seq(
  "org.apache.solr"      %  "solr-test-framework" % "5.2.1" % "test",     // must precede solrj in the classpath
  "org.scalatest"        %% "scalatest"           % "2.2.4" % "test",
  "com.novocode"         %  "junit-interface"     % "0.10"  % "test",
  "commons-logging"      %  "commons-logging"     % "1.1.3",
  "org.slf4j"            % "slf4j-log4j12"        % "1.7.7",
  "log4j"                % "log4j"                % "1.2.17",
  "org.apache.solr"      %  "solr-solrj"          % "5.2.1",
  "com.github.scopt"     %% "scopt"               % "3.3.0"
)
