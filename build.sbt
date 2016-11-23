import sbt.Keys._

lazy val akkaVersion = "2.4.12"

lazy val root =
  (project in file("."))
    .settings(
      name := "ex",
      version := "0.1.0",
      scalaVersion := "2.11.8",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream" % akkaVersion,
        "org.scodec" %% "scodec-bits" % "1.1.2",
        "org.scodec" %% "scodec-core" % "1.10.3",
        "org.scodec" %% "scodec-spire" % "0.4.0",
        "org.spire-math" %% "spire" % "0.11.0",
        "com.typesafe" % "config" % "1.3.1",
        "org.scalatest" %% "scalatest" % "3.0.1" % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
      ),
      mainClass in Compile := Some("aggregator.Main")
    )



