package com.spark.knowledge.datastorage

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MongoDB {

  def withDatabase(
                    database: String)(implicit spark: SparkSession =
  SparkSession.builder().getOrCreate()): MongoDB = {
    new MongoDB(database)
  }
}


class MongoDB(database: String) extends Serializable{

  /** Save DataFrame to MongoDB table.
    * @param df            DataFrame to be persisted to MongoDB table
    * @param collectionName     MongoDB Collection name
    * @param saveMode      default: SaveMode.Append. Overwrite mode will truncate table
    */
  def save(df: DataFrame,
           collectionName: String,
           saveMode: SaveMode = SaveMode.Append): Unit = {
    val writer = df.write.format("mongo").option("database", database).option("collection", collectionName).mode(saveMode)
    if (saveMode == SaveMode.Overwrite)
      writer.mode(SaveMode.Overwrite).save()
    else writer.save()
  }

  /** Read view of a Mongo collection as DataFrame
    *
    * @param collectionName
    * @return DataFrame
    */
  def read(collectionName: String): DataFrame = {
    SparkSession
      .builder()
      .getOrCreate()
      .read
      .format("mongo")
      .option("database",database)
      .option("collection", collectionName)
      .load()
  }


}
