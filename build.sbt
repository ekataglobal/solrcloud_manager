name := "solrcloud_manager"

organization := "com.whitepages"

version := "0.1"

scalaVersion := "2.10.3"

fork in Test := true

javaOptions += "-ea"

resolvers += "Restlet Repository" at "http://maven.restlet.org"

libraryDependencies ++= Seq(
  "org.apache.solr"      %  "solr-test-framework" % "4.9.0" % "test",     // must precede solrj in the classpath
  "org.scalatest"        %% "scalatest"           % "2.1.6" % "test",
  "com.novocode"         %  "junit-interface"     % "0.10"  % "test",
  "commons-logging"      %  "commons-logging"     % "1.1.3",
  "org.slf4j"            % "slf4j-log4j12"        % "1.7.7",
  "log4j"                % "log4j"                % "1.2.17",
  "org.apache.solr"      %  "solr-solrj"          % "4.9.0",
  "com.github.scopt"     %% "scopt"               % "3.2.0"
)
