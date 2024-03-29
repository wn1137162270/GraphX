name := "GraphX"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.4"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" % "compile"
