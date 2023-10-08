ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.2"

lazy val root = (project in file("."))
  .settings(
    name := "TestSparkApp"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.0"
