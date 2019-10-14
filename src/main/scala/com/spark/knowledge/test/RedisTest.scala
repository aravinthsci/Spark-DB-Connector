package com.spark.knowledge.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import  com.spark.knowledge.datastorage.RedisDB

object RedisTest extends App{

  lazy val sparkConf = new SparkConf()
    // initial redis host - can be any node in cluster mode
    .set("redis.host", "localhost")
    // initial redis port
    .set("redis.port", "6379")
    // optional redis AUTH password
    .set("redis.auth", "")
    .setAppName("Redis Test")
    .setMaster("local")
    .set("spark.cores.max", "2")

  val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val sales_df = sparkSession.read.option("header",true).option("inferSchema",true).csv("/data/big data/Sale.csv")

    //Load redis object
  val redisObj = new RedisDB()

  //save to redis
  redisObj.save(sales_df,"sales_report","id")

  //read data from redis

  val sales = redisObj.read("sales_report","id")

  sales.show()





}
