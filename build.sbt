name := "MyApp"

version := "1.0"

scalaVersion := "2.11.8"

publishMavenStyle := true
	
publishArtifact in Test := false

pomIncludeRepository := { x => false }

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.1"

libraryDependencies += "org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.5.0"

libraryDependencies += "org.json4s" % "json4s-jackson_2.11" % "3.2.10"

libraryDependencies += "org.mongodb" % "mongo-java-driver" % "3.2.1"

