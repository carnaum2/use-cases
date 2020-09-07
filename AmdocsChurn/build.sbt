name := "AmdocsChurn"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"  % "provided"

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.1.0" % "provided"

libraryDependencies += "com.databricks" %% "spark-csv" % "1.2.0"

libraryDependencies += "joda-time" % "joda-time" % "2.9.9"