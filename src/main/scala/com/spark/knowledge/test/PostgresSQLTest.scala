package com.spark.knowledge.test

import com.spark.knowledge.datastorage.PostgresSQL
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object PostgresSQLTest extends App{

  lazy val sparkConf = new SparkConf()
    .setAppName("Postgres Test")
    .setMaster("local")
    .set("spark.cores.max", "2")

  val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val postgresObj = PostgresSQL.withConnection(url="jdbc:postgresql://localhost:5432/", user="postgres",password="docker")

  val sales_df = sparkSession.read.option("header",true).option("inferSchema",true).csv("/data/big data/Sale.csv")

  postgresObj.save(sales_df,"postgres","sales")

  val df = postgresObj.read("postgres", "sales")

  df.show()

}
