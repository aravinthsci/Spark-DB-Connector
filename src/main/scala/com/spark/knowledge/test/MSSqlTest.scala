package com.spark.knowledge.test

import com.spark.knowledge.datastorage.MSSql
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object MSSqlTest extends App{

  lazy val sparkConf = new SparkConf()
    .setAppName("MSSql Test")
    .setMaster("local")
    .set("spark.cores.max", "2")

  val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

    val mssqlObj = MSSql.withConnection(driver="com.microsoft.sqlserver.jdbc.SQLServerDriver", url="jdbc:sqlserver://localhost:1433", user="user", password="password")

  val sales_df = sparkSession.read.option("header",true).option("inferSchema",true).csv("/data/big data/Sale.csv")
  sales_df.show(2)
  //saving dataframe to mssql
  mssqlObj.save(sales_df, database ="CLOUDEDH.dbo", tableName= "sales")

  //Reading from mssql

  val df = mssqlObj.read("CLOUDEDH.dbo", "sales")

  df.show()


}
