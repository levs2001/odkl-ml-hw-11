ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "WordCountTest"
  )

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8" % Provided,
  "org.apache.spark" %% "spark-core" % "2.4.8" % Test,
  "org.apache.spark" %% "spark-core" % "2.4.8" % Test classifier "tests",
  "org.apache.spark" %% "spark-streaming" % "2.4.8" % Provided,
  "org.apache.spark" %% "spark-streaming" % "2.4.8" % Test,
  "org.apache.spark" %% "spark-streaming" % "2.4.8" % Test classifier "tests",
  "org.scalatest" %% "scalatest" % "3.0.3" % Test
)

Test / run / javaOptions ++= Seq(
  "-Dspark.master=local",
  "-Dlog4j.configuration=log4j.properties"
)