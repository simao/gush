name := "gush"

scalaVersion := "2.11.7"

version := "0.0.2"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", " -language:implicitConversions")

testOptions in Test += Tests.Argument("-l", "IgnoreTest")

resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"

libraryDependencies += "com.github.shyiko" % "mysql-binlog-connector-java" % "0.1.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "org.yaml" % "snakeyaml" % "1.13"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.1"

libraryDependencies += "com.foundationdb" % "fdb-sql-parser" % "1.0.17"

libraryDependencies += "org.mockito" % "mockito-all" % "1.10.8"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.2"

libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.4.2"

libraryDependencies += "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.2"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.4.2"
