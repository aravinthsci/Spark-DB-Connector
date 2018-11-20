package com.spark.knowledge.datastorage

/**
  * Created by aravinth on 18/11/2018.
  */


import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MemSQL {

  /** Create MemSQL helper utility with a keyspace name.
    * */
  def withKeyspace(
                    keyspace: String)(implicit spark: SparkSession =
  SparkSession.builder().getOrCreate()): MemSQL = {
    new MemSQL(keyspace, spark.sparkContext)
  }

}


class MemSQL(keyspace: String, sc: SparkContext) extends Serializable {


  /** Save DataFrame to MemSQL table.
    * @param df            DataFrame to be persisted to MemSQL table
    * @param tableName     MemSQL table name
    * @param saveMode      default: SaveMode.Append. Overwrite mode will truncate table
    */
  def save(df: DataFrame,
           tableName: String,
           saveMode: SaveMode = SaveMode.Append): Unit = {
    val writer = df.write.format("com.memsql.spark.connector").mode("Append")
    if (saveMode == SaveMode.Overwrite)
      writer.mode("Overwrite").save(keyspace + "." + tableName)
    else writer.save(keyspace + "." + tableName)
  }

  /** Read view of a MemSQL table as DataFrame
    *
    * @param tableName
    * @return DataFrame
    */
  def read(tableName: String): DataFrame = {
    SparkSession
      .builder()
      .getOrCreate()
      .read
      .format("com.memsql.spark.connector")
      .options(Map("path" -> (keyspace + "." + tableName)))
      .load()
  }

}