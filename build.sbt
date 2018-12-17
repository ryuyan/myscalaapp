name := "myscalaapp"

version := "0.1"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies +=  "org.apache.spark" %% "spark-sql" % "2.4.0"
