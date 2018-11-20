package com.spark.knowledge.filesystem

/**
  * Created by aravinth on 18/11/2018.
  */


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession

object URI {
  def apply(uri: String): java.net.URI =
    new org.apache.hadoop.fs.Path(uri).toUri
}

object FileSystemUtil {
  def apply(spark: SparkSession): FileSystemUtil =
    apply(spark.sparkContext.hadoopConfiguration)

  def apply(cfg: Configuration): FileSystemUtil = new FileSystemUtil(cfg)
}

class FileSystemUtil(val cfg: Configuration) {
  def exists(filePath: String): Boolean =
    exists(getFileSystem(filePath), filePath)

  private def exists(fileSystem: => FileSystem, filePath: String): Boolean =
    filePath.trim.nonEmpty && fileSystem.exists(new Path(filePath))

  def delete(filePath: String): Boolean =
    delete(getFileSystem(filePath), filePath)

  private def delete(fileSystem: FileSystem, filePath: String): Boolean =
    filePath.trim.nonEmpty && fileSystem.delete(new Path(filePath), true)

  def read(uri: String): FSDataInputStream =
    getFileSystem(uri).open(new org.apache.hadoop.fs.Path(uri))

  def list(uri: String): Array[String] =
    getFileSystem(uri)
      .listStatus(new org.apache.hadoop.fs.Path(uri))
      .map(_.getPath.toString)

  /**
    * Converts RemoteIterator from Hadoop to Scala Iterator
    * @param underlying The RemoteIterator that needs to be wrapped
    * @tparam T Items inside the iterator
    * @return Standard Scala Iterator
    */
  implicit def convertToScalaIterator[T](
                                          underlying: RemoteIterator[T]): Iterator[T] = {
    case class wrapper(underlying: RemoteIterator[T]) extends Iterator[T] {
      override def hasNext = underlying.hasNext
      override def next = underlying.next
    }
    wrapper(underlying)
  }

  def globList(uri: String, recursive: Boolean): Array[String] = {
    import scala.collection.JavaConversions.asScalaIterator

    val fileSystem = getFileSystem(uri)
    Option(fileSystem.globStatus(new org.apache.hadoop.fs.Path(uri))) match {
      case None => Array.empty
      case Some(x) =>
        x.flatMap({
          case x: FileStatus if x.isDirectory =>
            fileSystem
              .listFiles(new org.apache.hadoop.fs.Path(x.getPath.toString),
                recursive)
              .map(_.getPath.toString)
          case x: FileStatus => Some(x.getPath.toString)
        })
    }
  }

  def getFileStatus(uri: String): FileStatus = {
    getFileSystem(uri).getFileStatus(new org.apache.hadoop.fs.Path(uri))
  }

  def getFileSystem(url: String): FileSystem = {
    val uri = URI(url)
    uri.getScheme match {
      case "s3" | "s3n" | "s3a" | "hdfs" => getS3FileSystem(url)
      case "file" | null                 => getFileSystem
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot handle `${uri.getScheme}`")
    }
  }

  def getFileSystem: FileSystem =
    FileSystem.get(cfg)

  private def getS3FileSystem(url: String): FileSystem =
    FileSystem.get(new org.apache.hadoop.fs.Path(getBucket(url)).toUri, cfg)

  def getBucket(s3url: String): String = {
    val index = s3url.indexOf("/", s3url.indexOf("//") + 2)
    s3url.substring(0, if (index > 0) index else s3url.length)
  }
}
