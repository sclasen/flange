import sbt._

class FlangeProject(info: ProjectInfo) extends DefaultProject(info) {


  object Repositories {
    lazy val AkkaRepo = MavenRepository("Akka Repository", "http://akka.io/repository")
    lazy val ScalaToolsRepo = MavenRepository("Scala-Tools Repo", "http://scala-tools.org/repo-releases")
    lazy val ScalaToolsSnapshotRepo = MavenRepository("Scala-Tools Snapshot Repo", "http://scala-tools.org/repo-snapshots")
    lazy val CodehausRepo = MavenRepository("Codehaus Repo", "http://repository.codehaus.org")
    lazy val LocalMavenRepo = MavenRepository("Local Maven Repo", (Path.userHome / ".m2" / "repository").asURL.toString)
    lazy val GuiceyFruitRepo = MavenRepository("GuiceyFruit Repo", "http://guiceyfruit.googlecode.com/svn/repo/releases/")
    lazy val JBossRepo = MavenRepository("JBoss Repo", "http://repository.jboss.org/nexus/content/groups/public/")
    lazy val JavaNetRepo = MavenRepository("java.net Repo", "http://download.java.net/maven/2")
    lazy val SonatypeSnapshotRepo = MavenRepository("Sonatype OSS Repo", "http://oss.sonatype.org/content/repositories/releases")
    lazy val SunJDMKRepo = MavenRepository("Sun JDMK Repo", "http://wp5.e-taxonomy.eu/cdmlib/mavenrepo")
    lazy val ClojarsRepo = MavenRepository("Clojars Repo", "http://clojars.org/repo")
    lazy val ScalaToolsRelRepo = MavenRepository("Scala Tools Releases Repo", "http://scala-tools.org/repo-releases")
    lazy val ForceRelRepo = MavenRepository("Force releases", "http://repo.t.salesforce.com/archiva/repository/releases")
    lazy val ForceSnaplRepo = MavenRepository("Force snapshots", "http://repo.t.salesforce.com/archiva/repository/snapshots")
    lazy val javanetRepo = MavenRepository("javanetrepo", "http://download.java.net/maven/2/")
  }

  import Repositories._

  lazy val nettyModuleConfig = ModuleConfiguration("org.jboss.netty", JBossRepo)
  lazy val akkaoduleConfig = ModuleConfiguration("se.scalablesolutions.akka", AkkaRepo)
  lazy val scalaTestModuleConfig = ModuleConfiguration("org.scalatest", ScalaToolsSnapshots)
  val localMavenRepo = LocalMavenRepo

  // Second exception, also fast! ;-)

  // -------------------------------------------------------------------------------------------------------------------
  // Versions
  // -------------------------------------------------------------------------------------------------------------------

  lazy val AKKA_VERSION = "1.1"
  lazy val CAMEL_VERSION = "2.7.0"
  lazy val SCALATEST_VERSION = "1.4-SNAPSHOT"
  lazy val SLF4J_VERSION = "1.6.0"

  // -------------------------------------------------------------------------------------------------------------------
  // Dependencies
  // -------------------------------------------------------------------------------------------------------------------

  object Dependencies {

    // Compile
    lazy val akka_actor = "se.scalablesolutions.akka" % "akka-actor" % AKKA_VERSION % "compile" withSources ()
   //ApacheV2
    lazy val akka_slf4j = "se.scalablesolutions.akka" % "akka-slf4j" % AKKA_VERSION % "compile"
    //ApacheV2
    lazy val camel_netty = "org.apache.camel" % "camel-netty" % CAMEL_VERSION % "compile" withSources ()
    //ApacheV2
    lazy val netty = "org.jboss.netty" % "netty" % "3.2.3.Final" % "compile" withSources ()
    //ApacheV2
    lazy val protobuf = "com.google.protobuf" % "protobuf-java" % "2.4.0a" withSources ()
    //New BSD
    lazy val slf4_api = "org.slf4j" % "slf4j-api" % SLF4J_VERSION % "compile"
    lazy val slf4_jcl = "org.slf4j" % "jcl-over-slf4j" % SLF4J_VERSION % "compile"
    lazy val slf4_jul = "org.slf4j" % "jul-to-slf4j" % SLF4J_VERSION % "compile"
    lazy val slf4_log4j = "org.slf4j" % "log4j-over-slf4j" % SLF4J_VERSION % "compile"
    // MIT
    // Test
    lazy val scalatest = "org.scalatest" % "scalatest" % SCALATEST_VERSION % "test"
    //ApacheV2
    lazy val logback = "ch.qos.logback" % "logback-classic" % "0.9.28" % "compile"

  }


  override def ivyXML = <dependencies>
      <exclude artifact="slf4j-simple"/>
      <exclude artifact="commons-logging"/>
  </dependencies>

  val protobuf = Dependencies.protobuf
  val akka_actor = Dependencies.akka_actor
  val netty = Dependencies.netty
  val akka_slf4j = Dependencies.akka_slf4j
  val slf4j_api = Dependencies.slf4_api
  val logback = Dependencies.logback
  val scalatest = Dependencies.scalatest


}