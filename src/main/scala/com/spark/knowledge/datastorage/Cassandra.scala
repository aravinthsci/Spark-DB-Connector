package com.spark.knowledge.datastorage

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnector, _}
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
  * Created by aravinth on 18/11/2018.
  */


object Cassandra {

  /** Create Cassandra helper utility with a keyspace name.
    * */
  def withKeyspace(
                    keyspace: String)(implicit spark: SparkSession =
  SparkSession.builder().getOrCreate()): Cassandra = {
    new Cassandra(keyspace, spark.sparkContext)
  }
}

class Cassandra(keyspace: String, sc: SparkContext) extends Serializable {

  lazy val connector: CassandraConnector = CassandraConnector(sc)
  lazy val cassandraVersion = getCassandraVersion

  /** Save DataFrame to Cassandra table.
    * @param df            DataFrame to be persisted to Cassandra table
    * @param tableName     Cassandra table name
    * @param saveMode      default: SaveMode.Append. Overwrite mode will truncate table
    */
  def save(df: DataFrame,
           tableName: String,
           saveMode: SaveMode = SaveMode.Append): Unit = {
    val writer = df.write.cassandraFormat(tableName, keyspace).mode(saveMode)
    if (saveMode == SaveMode.Overwrite)
      writer.option("confirm.truncate", "true").save()
    else writer.save()
  }

  /** Read view of a Cassandra table as DataFrame
    *
    * @param tableName
    * @return DataFrame
    */
  def read(tableName: String): DataFrame = {
    SparkSession
      .builder()
      .getOrCreate()
      .read
      .cassandraFormat(tableName, keyspace)
      .load()
  }

  /** Read view of a Cassandra table as CassandraRDD
    *
    * @param tableName
    * @return CassandraTableScanRDD[CassandraRow]
    */
  def readNative(tableName: String): CassandraTableScanRDD[CassandraRow] = {
    sc.cassandraTable(keyspace, tableName)
  }

   def getCassandraVersion: String = {
    val rs = connector.withSessionDo(
      _.execute("SELECT release_version FROM system.local"))
    return rs.one().getString("release_version")
  }

}