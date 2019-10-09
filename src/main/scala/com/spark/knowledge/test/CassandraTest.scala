package com.spark.knowledge.test
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import  com.spark.knowledge.datastorage.Cassandra

object CassandraTest extends App{

  lazy val sparkConf = new SparkConf()
    .set("spark.cassandra.connection.host", "localhost")
    .set("spark.cassandra.auth.username", "cassandra")
    .set("spark.cassandra.auth.password", "cassandra")
    .setAppName("Learn Spark")
    .setMaster("local")
    .set("spark.cores.max", "2")

   val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val cassObj = Cassandra.withKeyspace("test")

  println(cassObj.getCassandraVersion)

  val sales_df = sparkSession.read.option("header",true).option("inferSchema",true).csv("/data/big data/Sale.csv")
  //saving Data to Cassandra
  cassObj.save(sales_df,"sales")
  //reading Data from Cassandra
  val df = cassObj.read("sales")
    df.show()

}
