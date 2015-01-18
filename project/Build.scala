import sbt._
import Keys._
import xerial.sbt.Sonatype._
import SonatypeKeys._


object Cassovary extends Build {

  val fastUtilsDependency = "it.unimi.dsi" % "fastutil" % "6.6.0"

  val sharedSettings = Seq(
    version := "3.4.0",
    organization := "com.twitter",
    scalaVersion := "2.9.3",
    retrieveManaged := true,
    crossScalaVersions := Seq("2.9.3","2.10.3"),
    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "11.0.2",
      fastUtilsDependency % "provided",
      "org.mockito" % "mockito-all" % "1.8.5" % "test",
      "com.twitter" %% "util-core" % "6.16.0" cross CrossVersion.binaryMapped {
        case "2.9.3" => "2.9.2"
        case x if x startsWith "2.10" => "2.10"
        case x => x
      } excludeAll (
          ExclusionRule(organization = "junit"),
          ExclusionRule(organization = "org.scala-tools.testing"),
          ExclusionRule(organization = "org.mockito")),
      "com.twitter" %% "ostrich" % "9.1.0" cross CrossVersion.binaryMapped {
        case "2.9.3" => "2.9.2"
        case x if x startsWith "2.10" => "2.10"
        case x => x
      },
      "com.twitter" %% "util-logging" % "6.12.1" cross CrossVersion.binaryMapped {
        case "2.9.3" => "2.9.2"
        case x if x startsWith "2.10" => "2.10"
        case x => x
      },
      "org.scalatest" %% "scalatest" % "1.9.2" % "test" cross CrossVersion.binaryMapped {
              case "2.9.3" => "2.9.2"
              case x if x startsWith "2.10" => "2.10"
              case x => x
      },
      "junit" % "junit" % "4.10" % "test",
      "com.twitter.common" % "metrics" % "0.0.29"
    ),
    resolvers += "twitter repo" at "http://maven.twttr.com",

    scalacOptions ++= Seq("-encoding", "utf8"),
    scalacOptions += "-deprecation",

    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
    javacOptions in doc := Seq("-source", "1.6"),

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
    settings = Project.defaultSettings ++ sharedSettings
  ).settings(
    name := "cassovary-examples",
    libraryDependencies += fastUtilsDependency
  ).dependsOn(cassovaryCore)

  lazy val cassovaryBenchmarks = Project(
    id = "cassovary-benchmarks",
    base = file("cassovary-benchmarks"),
    settings = Project.defaultSettings ++ sharedSettings
  ).settings(
      name := "cassovary-benchmarks",
      libraryDependencies ++= Seq(
        fastUtilsDependency,
        "com.twitter" %% "util-app" % "6.12.1" cross CrossVersion.binaryMapped {
          case "2.9.3" => "2.9.2"
          case x if x startsWith "2.10" => "2.10"
          case x => x
        }
      )
  ).dependsOn(cassovaryCore)

  lazy val cassovaryServer = Project(
    id = "cassovary-server",
    base = file("cassovary-server"),
    settings = Project.defaultSettings ++ sharedSettings
  ).settings(
      name := "cassovary-server",
      libraryDependencies ++= Seq(
        fastUtilsDependency,
        "com.twitter" %% "finagle-http" % "6.20.0" cross CrossVersion.binaryMapped {
          case "2.9.3" => "2.9.2"
          case x if x startsWith "2.10" => "2.10"
          case x => x
        },
        "com.twitter" %% "finagle-ostrich4" % "6.20.0"  cross CrossVersion.binaryMapped {
          case "2.9.3" => "2.9.2"
          case x if x startsWith "2.10" => "2.10"
          case x => x
        },
        "com.twitter" % "twitter-server" % "1.9.0"
      )
  ).dependsOn(cassovaryCore)

}
