name := "Spark-Scala-Ex"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.3.3"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.1",
  "org.scala-lang" %% "scala-pickling" % "0.9.1",
  "com.google.code.gson" % "gson" % "1.7.1"


)







