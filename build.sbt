name := "ex1"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.4"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.12.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.12.1"