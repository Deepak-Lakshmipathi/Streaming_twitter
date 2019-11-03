name := "Streaming_twitter"

version := "0.1"

scalaVersion := "2.11.1"


// spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"

// spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"

// spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.4" % "provided"

// spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.4" % "provided"

// spark-streaming-twitter
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"

// https://mvnrepository.com/artifact/org.twitter4j/twitter4j-stream
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.7"
