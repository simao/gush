name := "gush"

scalaVersion := "2.11.2"

version := "0.1"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"

libraryDependencies += "com.github.shyiko" % "mysql-binlog-connector-java" % "0.1.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.0" % "test"

libraryDependencies += "com.espertech" % "esper" % "4.6.0"

libraryDependencies += "com.netflix.rxjava" % "rxjava-scala" % "0.19.2"

libraryDependencies += "org.yaml" % "snakeyaml" % "1.13"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.1"

libraryDependencies += "com.jcraft" % "jsch" % "0.1.51"

