package com.spark.knowledge.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SparkSession, SaveMode}
import com.spark.knowledge.datastorage.MongoDB

object MongoDBTest extends App{

  lazy val sparkConf = new SparkConf()
    .setAppName("MongoDB Test")
    .set("spark.mongodb.output.uri","mongodb://localhost:27098")
    .set("spark.mongodb.input.uri", "mongodb://localhost:27098")
    .setMaster("local")
    .set("spark.cores.max", "2")

  val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val sales_df = sparkSession.read.option("header",true).option("inferSchema",true).csv("/data/big data/Sale.csv")

  //creating object for MongoDB
  val mongoObj = MongoDB.withDatabase("test")

//saving dataframe as monogDB collection
  mongoObj.save(sales_df,"sales",SaveMode.Overwrite)

//Reading monogDB collection as dataframe
  val df = mongoObj.read("sales")

  df.show()

}
