package com.spark.knowledge.datastorage

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by aravinth on 14/10/2019.
  */

object Mysql {

  def withConnection(
                    url: String, user: String, password: String )(implicit spark: SparkSession =
  SparkSession.builder().getOrCreate()): Mysql = {
    new Mysql(url, user, password)
  }

}


class Mysql(url: String, user: String, password: String) extends Serializable {

  /** Save DataFrame to MySql table.
    * @param df            DataFrame to be persisted to MySql table
    * @param tableName     MySql table name
    * @param saveMode      default: SaveMode.Append. Overwrite mode will truncate table
    */
  def save(df: DataFrame,
           database: String,
           tableName: String,
           saveMode: SaveMode = SaveMode.Append): Unit = {
    val writer = df.write.format("jdbc").option("url", url)
      .option("dbtable", database + "." + tableName)
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Append)
    if (saveMode == SaveMode.Overwrite)
      writer.mode(SaveMode.Overwrite).save()
    else writer.save()
  }

  /** Read view of a Mysql table as DataFrame
    *
    * @param tableName
    * @return DataFrame
    */
  def read(database: String, tableName: String): DataFrame = {
    SparkSession
      .builder()
      .getOrCreate()
      .read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", database + "." + tableName)
      .option("user", user)
      .option("password", password)
      .load()
  }

}
