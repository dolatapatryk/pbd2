name := "Authorities"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
