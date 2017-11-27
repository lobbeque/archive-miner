import AssemblyKeys._ 

assemblySettings

name := "archive-miner"

organization := "qlobbe"

version := "1.0.0"

description := "Data Mining tools for web archives DAFF & WARC"

publishMavenStyle := true

autoScalaLibrary := false

mainClass in Compile := Some("qlobbe.ArchiveReader")

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-feature")

javaOptions += "-Xmx15G"

resolvers += "Restlet Repository" at "http://maven.restlet.org"

libraryDependencies ++= Seq(
   "com.google.guava" % "guava" % "14.0",
   "org.restlet.jee" % "org.restlet.ext.servlet" % "2.3.0",
   "org.restlet.jee" % "org.restlet" % "2.3.0",
   "org.scala-lang" % "scala-reflect" % "2.11.8",
   "org.scala-lang" % "scala-library" % "2.11.8",
   "org.apache.spark" % "spark-core_2.10" % "2.2.0",
   "org.apache.solr" % "solr-core" % "6.6.1",
   "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.7.2",
   "org.apache.commons" % "commons-lang3" % "3.0",
   "com.googlecode.json-simple" % "json-simple" % "1.1"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
  	case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  	case PathList(ps @ _*) if ps.last endsWith ".SF" => MergeStrategy.discard
    case PathList(ps @ _*) => MergeStrategy.first 
    case x => old(x)
  }
}

