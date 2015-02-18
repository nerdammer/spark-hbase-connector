organization := "it.nerdammer.bigdata"

name := "spark-hbase-connector"

version := "0.9.4-SNAPSHOT"

scalaVersion := "2.10.4"


libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test",
  "org.apache.spark" % "spark-core_2.10" % "1.2.1" % "provided",
  "org.apache.hbase" % "hbase-common" % "0.98.10.1-hadoop2" % "provided",
  "org.apache.hbase" % "hbase-client" % "0.98.10.1-hadoop2" % "provided",
  "org.apache.hbase" % "hbase-server" % "0.98.10.1-hadoop2" % "provided"
)

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra :=
  <url>http://www.nerdammer.it</url>
    <licenses>
      <license>
        <name>Apache License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:nerdammer/spark-hbase-connector</url>
      <connection>scm:git:git@github.com:nerdammer/spark-hbase-connector.git</connection>
    </scm>
    <developers>
      <developer>
        <id>nibbio84</id>
        <name>Nicola Ferraro</name>
        <url>http://www.nerdammer.it</url>
      </developer>
    </developers>

