organization := "it.nerdammer.bigdata"

name := "spark-hbase-connector"

version := "1.0.2"

scalaVersion := "2.10.4"

libraryDependencies +=  "org.apache.spark" % "spark-core_2.10" % "1.2.0" % "provided"

libraryDependencies +=  "org.apache.hbase" % "hbase-common" % "0.98.11-hadoop2" excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"), ExclusionRule(organization = "org.mortbay.jetty", name="jetty"), ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5"))

libraryDependencies +=  "org.apache.hbase" % "hbase-client" % "0.98.11-hadoop2" excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"), ExclusionRule(organization = "org.mortbay.jetty", name="jetty"), ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5"))

libraryDependencies +=  "org.apache.hbase" % "hbase-server" % "0.98.11-hadoop2" excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"), ExclusionRule(organization = "org.mortbay.jetty", name="jetty"), ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5"))


libraryDependencies +=  "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"


spName := "nerdammer/spark-hbase-connector"

sparkVersion := "1.2.0"

credentials += Credentials(Path.userHome / ".ivy2" / ".githubcredentials")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

spIncludeMaven := true



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

