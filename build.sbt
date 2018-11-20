name := "Spark-knowledge"

version := "0.1"

scalaVersion := "2.11.8"

lazy val sparkVersion = "2.1.1"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

resolvers ++= Seq(
  "apache-snapshots" at "https://repo1.maven.org/maven2/"
)


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

//cassandra dependeincies
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.2"
libraryDependencies += "com.twitter" % "jsr166e" % "1.1.0"

//memsql dependencies
libraryDependencies  += "com.memsql" %% "memsql-connector" % "2.0.6"

