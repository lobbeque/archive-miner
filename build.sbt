import AssemblyKeys._ 

assemblySettings

name := "archive-miner"

organization := "qlobbe"

version := "1.0.0"

description := "Data Mining tools for web archives DAFF & WARC"

publishMavenStyle := true

autoScalaLibrary := false

mainClass in Compile := Some("qlobbe.ArchiveReader")

scalaVersion := "2.10.5"

scalacOptions ++= Seq("-feature")

javaOptions += "-Xmx15G"

resolvers += "Restlet Repository" at "http://maven.restlet.org"

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "14.0",
   "org.restlet.jee" % "org.restlet.ext.servlet" % "2.3.0",
   "org.restlet.jee" % "org.restlet" % "2.3.0",
   "org.scala-lang" % "scala-reflect" % "2.10.4",
   "org.scala-lang" % "scala-library" % "2.10.5",
   "org.apache.spark" % "spark-core_2.10" % "1.6.1",
   "org.apache.spark" % "spark-streaming_2.10" % "1.6.1",
   "org.apache.solr" % "solr-core" % "5.4.1"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
  	case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  	case PathList(ps @ _*) if ps.last endsWith ".SF" => MergeStrategy.discard
    case PathList(ps @ _*) => MergeStrategy.first 
    case x => old(x)
  }
}

