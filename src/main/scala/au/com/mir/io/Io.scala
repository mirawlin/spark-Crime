package au.com.mir.io

import org.apache.spark.sql.{ DataFrame, SparkSession }

object Io {

  def readCsv(fileName: String)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession
      .read
      .option("header", "true")
      .csv(fileName)
  }

  def writeParquet(df: DataFrame, path: String)(implicit sparkSession: SparkSession): Unit = {
    df
      .write
      .parquet(path)
  }
}
