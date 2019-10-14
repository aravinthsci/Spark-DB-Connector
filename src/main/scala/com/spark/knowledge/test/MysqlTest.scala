package com.spark.knowledge.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.spark.knowledge.datastorage.Mysql

object MysqlTest extends App{

  lazy val sparkConf = new SparkConf()
    .setAppName("Mysql Test")
    .setMaster("local")
    .set("spark.cores.max", "2")

  val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val mysqlObj = Mysql.withConnection(url="jdbc:mysql://localhost:3306", user="root",password="root")

   val sales_df = sparkSession.read.option("header",true).option("inferSchema",true).csv("/data/big data/Sale.csv")

//saving dataframe to mysql
  mysqlObj.save(sales_df, database ="testdb", tableName= "sales")

//Reading from mysql

  val df = mysqlObj.read("testdb", "sales")

  df.show()


}
