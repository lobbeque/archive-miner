name := "archive-miner"

organization := "qlobbe"

version := "1.0.0"

description := "Data Mining tools for web archives DAFF & WARC"

publishMavenStyle := true

autoScalaLibrary := false

mainClass in Compile := Some("qlobbe.ArchiveReader")

libraryDependencies ++= Seq(
   "org.apache.spark" % "spark-core_2.10" % "1.6.0"
)
