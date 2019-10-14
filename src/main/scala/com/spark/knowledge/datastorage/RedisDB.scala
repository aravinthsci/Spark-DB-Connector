package com.spark.knowledge.datastorage

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by aravinth on 14/10/2019.
  */


class RedisDB extends Serializable {

  /** Save DataFrame to Redis db.
    * @param df            DataFrame to be persisted to Redis
    * @param tableName     Redis table name
    * @param saveMode      default: SaveMode.Append. Overwrite mode will truncate table
    */

  def save(df: DataFrame,
           tableName: String,
           keyColumn: String,
           saveMode: SaveMode = SaveMode.Append): Unit = {
    val writer = df.write.format("org.apache.spark.sql.redis").option("table", tableName).option("key.column", keyColumn).mode(saveMode)
    if (saveMode == SaveMode.Overwrite)
      writer.mode(SaveMode.Overwrite).save()
    else writer.save()
  }

  /** Read view of a Redis as DataFrame
    *
    * @param tableName
    * @return DataFrame
    */
  def read(tableName: String, keyColumn : String): DataFrame = {
    SparkSession
      .builder()
      .getOrCreate()
      .read
      .format("org.apache.spark.sql.redis")
      .option("table", tableName)
      .option("key.column", keyColumn)
      .load()
  }

}
