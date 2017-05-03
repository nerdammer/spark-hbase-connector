import sbt.ExclusionRule
name := "Spark-2.0-Scala-2.11-Sample"

version := "1.0"

scalaVersion := "2.11.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.0" excludeAll(ExclusionRule(organization="joda-time"), ExclusionRule(organization="org.slf4j"), ExclusionRule(organization="com.sun.jersey.jersey-test-framework"), ExclusionRule(organization="org.apache.hadoop"))
libraryDependencies += "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0" excludeAll(ExclusionRule(organization="joda-time"), ExclusionRule(organization="org.slf4j"))
libraryDependencies += "org.apache.commons" % "commons-exec" % "1.3"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


    