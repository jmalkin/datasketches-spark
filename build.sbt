name := "datasketches-spark"
version := "1.0-SNAPSHOT"
scalaVersion := "2.12.20"

organization := "org.apache.datasketches"
description := "The Apache DataSketches package for Spark"

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

inThisBuild(List(
  javacOptions ++= Seq("-source", "11", "-target", "11"),
  scalacOptions ++= Seq("-encoding", "UTF-8", "-release", "11")
))

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.12.6",
  "org.apache.spark" %% "spark-sql" % "3.4.4" % "provided",
  "org.apache.datasketches" % "datasketches-java" % "6.1.1" % "compile",
  "org.scalatest" %% "scalatest" % "3.2.19" % "test",
  "org.scalatestplus" %% "junit-4-13" % "3.2.19.0" % "test"
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint"
)

logBuffered in Test := false

//logLevel := Level.Warn

// Only show warnings and errors on the screen for compilations.
// This applies to both test:compile and compile and is Info by default
logLevel in compile := Level.Warn

// Level.INFO is needed to see detailed output when running tests
logLevel in test := Level.Info
