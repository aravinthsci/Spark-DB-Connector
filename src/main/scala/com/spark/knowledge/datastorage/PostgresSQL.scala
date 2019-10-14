package com.spark.knowledge.datastorage

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by aravinth on 14/10/2019.
  */

object PostgresSQL {

  def withConnection(
                      url: String, user: String, password: String )(implicit spark: SparkSession =
  SparkSession.builder().getOrCreate()): PostgresSQL = {
    new PostgresSQL(url, user, password)
  }

}


class PostgresSQL(url: String, user: String, password: String) extends Serializable {

  /** Save DataFrame to PostgresSQL table.
    * @param df            DataFrame to be persisted to PostgresSQL table
    * @param tableName     PostgresSQL table name
    * @param saveMode      default: SaveMode.Append. Overwrite mode will truncate table
    */
  def save(df: DataFrame,
           database: String,
           tableName: String,
           saveMode: SaveMode = SaveMode.Append): Unit = {
    val writer = df.write.format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", url + database)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Append)
    if (saveMode == SaveMode.Overwrite)
      writer.mode(SaveMode.Overwrite).save()
    else writer.save()
  }

  /** Read view of a PostgresSQL table as DataFrame
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
      .option("driver", "org.postgresql.Driver")
      .option("url", url + database)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .load()
  }

}
