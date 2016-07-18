name := "Sherlock"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.5.2" % "provided",
  "joda-time" % "joda-time" % "2.9.2"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"