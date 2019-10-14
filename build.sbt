name := "Spark-knowledge"

version := "0.1"

scalaVersion := "2.11.8"

lazy val sparkVersion = "2.4.4"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

resolvers ++= Seq(
  "apache-snapshots" at "https://repo1.maven.org/maven2/"
)


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

//cassandra dependeincies
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1"
libraryDependencies += "com.twitter" % "jsr166e" % "1.1.0"

//memsql dependencies
libraryDependencies  += "com.memsql" %% "memsql-connector" % "2.0.6"

//spark-redis dependencies
libraryDependencies += "com.redislabs" % "spark-redis" % "2.4.0"
libraryDependencies += "redis.clients" % "jedis" % "3.1.0-m1"

//MongoDB

libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.1"


