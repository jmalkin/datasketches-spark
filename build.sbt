/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import scala.xml.dtd.DEFAULT
import scala.io.Source

import BuildUtils._

val DEFAULT_SCALA_VERSION = "2.12.20"
val DEFAULT_SPARK_VERSION = "3.5.4"
val DEFAULT_JDK_VERSION = "17"

// Map of JVM version prefix to:
// (JVM major version, datasketches-java version)
// TODO: consider moving to external file
val jvmVersionMap = Map(
  "21" -> ("21", "8.0.0"),
  "17" -> ("17", "7.0.1"),
  "11" -> ("11", "6.2.0"),
  "8"  -> ("8",  "6.2.0"),
  "1.8" -> ("8", "6.2.0")
)

// version processing logic
val scalaVersion = settingKey[String]("The version of Scala")
val scalaVersionValue = sys.env.getOrElse("SCALA_VERSION", DEFAULT_SCALA_VERSION)

val sparkVersion = settingKey[String]("The version of Spark")
val sparkVersionValue = sys.env.getOrElse("SPARK_VERSION", DEFAULT_SPARK_VERSION)

val jvmFullVersion = settingKey[String]("The JVM version")
val jvmFullVersionValue = sys.props("java.version")

val jvmVersion = settingKey[String]("The JVM major version")
val jvmVersionValue = jvmVersionMap.collectFirst {
  case (prefix, (major, _)) if jvmFullVersionValue.startsWith(prefix) => major
}.getOrElse(DEFAULT_JDK_VERSION)

// look up the associated datasketches-java version
val dsJavaVersion = settingKey[String]("The DataSketches Java version")
val dsJavaVersionValue = jvmVersionMap.get(jvmVersionValue).map(_._2).getOrElse("ERROR")

lazy val copyDatasketchesDependencies = taskKey[Seq[File]]("Copy dependencies to a known location")

lazy val root = (project in file("."))
  .settings(
    name := "datasketches-spark",
    version := readVersion("version.cfg.in"),
    organization := "org.apache.datasketches",
    description := "The Apache DataSketches package for Spark",
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    homepage := Some(url("https://datasketches.apache.org/")),
    jvmVersion := jvmVersionValue,
    dsJavaVersion := dsJavaVersionValue,
    sparkVersion := sparkVersionValue,
    scalaVersion := scalaVersionValue,
    javacOptions ++= Seq("-source", jvmVersion.value, "-target", jvmVersion.value),
    scalacOptions ++= Seq("-encoding", "UTF-8", "-release", jvmVersion.value),
    Test / javacOptions ++= Seq("-source", jvmVersion.value, "-target", jvmVersion.value),
    Test / scalacOptions ++= Seq(
      "-encoding", "UTF-8",
      "-release", jvmVersion.value,
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint"
    ),
    libraryDependencies ++= Seq(
      "org.apache.datasketches" % "datasketches-java" % dsJavaVersion.value,
      "org.scala-lang" % "scala-library" % scalaVersion.value % "provided", // scala3-library may need to use %%
      ("org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided").cross(CrossVersion.for3Use2_13),
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "org.scalatestplus" %% "junit-4-13" % "3.2.19.0" % "test"
    ),
    copyDatasketchesDependencies := {
      // we want to copy non-provided/non-test dependencies to a known location
      // so that they can be obtained easily
      val targetLibDir = target.value / "lib"
      IO.createDirectory(targetLibDir)
      val dependencyJars = (Compile / dependencyClasspath).value.collect {
        case attr if (attr.data.getName.startsWith("datasketches-java") || attr.data.getName.startsWith("datasketches-memory"))
                      && attr.data.getName.endsWith(".jar") =>
          val file = attr.data
          val targetFile = targetLibDir / file.getName
          IO.copyFile(file, targetFile)
          targetFile
      }
      dependencyJars
    },
    Compile / packageBin := (Compile / packageBin).dependsOn(copyDatasketchesDependencies).value,
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
    // additional options for java 17
    Test / fork := {
      if (jvmVersion.value == "17") true
      else (Test / fork).value
    },
    Test / javaOptions ++= {
      if (jvmVersion.value == "17") {
        Seq("--add-modules=jdk.incubator.foreign",
            "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
        )
      } else {
        Seq.empty
      }
    },
    Test / logBuffered := false,
    // Level.INFO is needed to see detailed output when running tests
    Test / logLevel := Level.Info
  )
