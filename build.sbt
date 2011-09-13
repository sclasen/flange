name := "flange"

organization := "com.heroku.doozer"

version := "0.8-SNAPSHOT"

scalaVersion := "2.9.0-1"

compileOrder := CompileOrder.JavaThenScala

libraryDependencies ++= Seq(
    "se.scalablesolutions.akka" % "akka-actor" % "2.0-SNAPSHOT" withSources(),
    "se.scalablesolutions.akka" % "akka-slf4j" % "2.0-SNAPSHOT" withSources(),
    "org.jboss.netty" % "netty" % "3.2.5.Final" withSources(),
    "com.google.protobuf" % "protobuf-java"   % "2.4.1" withSources,
    "ch.qos.logback"      % "logback-classic" % "0.9.28"    % "runtime",
    "org.scalatest"           % "scalatest_2.9.0"     % "1.6.1"  % "test"
)
