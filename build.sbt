name := "ScalaSparkServer"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "com.twitter" %% "finagle-http" % "18.7.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-launcher" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % "2.2.1"
