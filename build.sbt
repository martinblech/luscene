name := "musicdb"

version := "0.1"

scalaVersion := "2.10.0"

libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-core" % "4.0.0",
  // test dependencies
  "org.specs2" %% "specs2" % "1.13" % "test",
  "org.mockito" % "mockito-core" % "1.9.5" % "test"
)

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "http://oss.sonatype.org/content/repositories/releases"
)
