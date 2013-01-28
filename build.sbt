name := "luscene"

version := "0.1.0"

scalaVersion := "2.10.0"

libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-core" % "4.0.0",
  "org.apache.lucene" % "lucene-analyzers-common" % "4.0.0",
  "com.twitter" % "util-eval" % "6.0.5",
  // test dependencies
  "org.specs2" %% "specs2" % "1.13" % "test",
  "org.mockito" % "mockito-core" % "1.9.5" % "test"
)

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "http://oss.sonatype.org/content/repositories/releases",
  "twitter" at "http://maven.twttr.com/"
)
