package au.com.mir

import org.apache.spark.sql.{ DataFrame, SparkSession }

object Io {

  def readCsv(fileName: String)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession
      .read
      .csv(fileName)
  }

  def writeParquet(df: DataFrame, path: String)(implicit sparkSession: SparkSession): Unit = {
    df
      .write
      .parquet(path)
  }
}
