
name := "newday"

version := "0.1"

scalaVersion := "2.12.10"

//crossScalaVersions := Seq("2.11.12", "2.12.12")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-core" % "3.0.0")

// https://mvnrepository.com/artifact/junit/junit
libraryDependencies += "junit" % "junit" % "4.10" % Test
