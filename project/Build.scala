import sbt._
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._
import xerial.sbt.Sonatype._


object Cassovary extends Build {

  val CassovaryLibraryVersion = "5.2.1"

  val utilVersion = "6.23.0"
  val fastUtilsDependency = "it.unimi.dsi" % "fastutil" % "6.6.0"

  def util(which: String) =
    "com.twitter" %% ("util-" + which) % utilVersion excludeAll(
        ExclusionRule(organization = "junit"),
        ExclusionRule(organization = "org.scala-tools.testing"),
        ExclusionRule(organization = "org.mockito"))

  val sharedSettings = Seq(
    version := CassovaryLibraryVersion,
    organization := "com.twitter",
    scalaVersion := "2.11.5",
    crossScalaVersions := Seq("2.10.4", "2.11.5"),
    retrieveManaged := true,
    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "16.0.1",
      fastUtilsDependency % "provided",
      "org.mockito" % "mockito-all" % "1.8.5" % "test",
      util("core"),
      util("logging"),
      "org.scalatest" %% "scalatest" % "2.2.3" % "test",
      "junit" % "junit" % "4.10" % "test",
      "com.twitter.common" % "metrics" % "0.0.29",
      "com.twitter" %% "finagle-stats" % "6.24.0",
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    ),
    resolvers += "twitter repo" at "http://maven.twttr.com",

    scalacOptions ++= Seq("-encoding", "utf8"),
    scalacOptions += "-deprecation",

    fork in run := true,
    javaOptions in run ++= Seq("-server"),
    outputStrategy := Some(StdoutOutput),

    // Sonatype publishing
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    publishMavenStyle := true,
    pomExtra := (
      <url>http://twitter.com/cassovary</url>
      <licenses>
        <license>
          <name>Apache 2</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
          <distribution>repo</distribution>
          <comments>A business-friendly OSS license</comments>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:twitter/cassovary.git</url>
        <connection>scm:git:git@github.com:twitter/cassovary.git</connection>
      </scm>
      <developers>
        <developer><id>pankaj</id><name>Pankaj Gupta</name><url>https://twitter.com/pankaj</url></developer>
        <developer><id>dongwang218</id><name>Dong Wang</name><url>https://twitter.com/dongwang218</url></developer>
        <developer><id>tao</id><name>Tao Tao</name><url>https://twitter.com/tao</url></developer>
        <developer><id>johnsirois</id><name>John Sirois</name><url>https://twitter.com/johnsirois</url></developer>
        <developer><id>aneeshs</id><name>Aneesh Sharma</name><url>https://twitter.com/aneeshs</url></developer>
        <developer><id>ashishgoel</id><name>Ashish Goel</name><url>https://twitter.com/ashishgoel</url></developer>
        <developer><id>4ad</id><name>Mengqiu Wang</name><url>https://twitter.com/4ad</url></developer>
        <developer><id>ningliang</id><name>Ning Liang</name><url>https://twitter.com/ningliang</url></developer>
        <developer><id>ajeet</id><name>Ajeet Grewal</name><url>https://twitter.com/ajeet</url></developer>
      </developers>),
    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  )

  val assemblySettings = Seq(
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false,
      includeDependency = false))

  lazy val root = Project(
    id = "cassovary",
    base = file("."),
    settings = Project.defaultSettings ++ sharedSettings ++ sonatypeSettings
  ).aggregate(
      cassovaryCore, cassovaryExamples, cassovaryBenchmarks, cassovaryServer
  )

  lazy val cassovaryCore = Project(
    id = "cassovary-core",
    base = file("cassovary-core"),
    settings = Project.defaultSettings ++ sharedSettings ++ sonatypeSettings
  ).settings(
    name := "cassovary-core"
  )

  lazy val cassovaryExamples = Project(
    id = "cassovary-examples",
    base = file("cassovary-examples"),
    settings = Project.defaultSettings ++ sharedSettings ++ assemblySettings
  ).settings(
        name := "cassovary-examples",
        libraryDependencies += fastUtilsDependency
  ).dependsOn(cassovaryCore)

  lazy val cassovaryBenchmarks = Project(
    id = "cassovary-benchmarks",
    base = file("cassovary-benchmarks"),
    settings = Project.defaultSettings ++ sharedSettings ++ assemblySettings
  ).settings(
      name := "cassovary-benchmarks",
      libraryDependencies ++= Seq(
        fastUtilsDependency,
        util("app")
      )
  ).dependsOn(cassovaryCore)

  lazy val cassovaryServer = Project(
    id = "cassovary-server",
    base = file("cassovary-server"),
    settings = Project.defaultSettings ++ sharedSettings ++ assemblySettings
  ).settings(
      name := "cassovary-server",
      libraryDependencies ++= Seq(
        fastUtilsDependency,
        "com.twitter" %% "finagle-http" % "6.24.0",
        "com.twitter" %% "twitter-server" % "1.9.0"
      )
  ).dependsOn(cassovaryCore)

}
