name := "gush"

scalaVersion := "2.10.3"

version := "0.1"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "com.typesafe" %% "scalalogging-log4j" % "1.0.1"

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.0-rc1"

libraryDependencies += "com.github.shyiko" % "mysql-binlog-connector-java" % "0.1.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "com.espertech" % "esper" % "4.6.0"

libraryDependencies += "com.netflix.rxjava" % "rxjava-scala" % "0.19.2"

libraryDependencies += "org.yaml" % "snakeyaml" % "1.13"

