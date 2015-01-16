name := "spark-hbase-connector"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.3" % "test"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.0" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "0.98.8-hadoop2" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "0.98.8-hadoop2" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "0.98.8-hadoop2" % "provided"