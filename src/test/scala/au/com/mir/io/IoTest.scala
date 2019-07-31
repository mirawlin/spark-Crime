package au.com.mir.io

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, WordSpec}

class IoTest extends WordSpec with Matchers {

  def createSparkSession(appName: String = getClass.getName): SparkSession = {
    SparkSession.clearActiveSession()
    val globalSession = SparkSession.getDefaultSession.getOrElse(SparkSession.builder()
      .appName("Global session")
      .master("local[*]")
      .config("avro.mapred.ignore.inputs.without.extension", value = false)
      .config("spark.debug.maxToStringFields", 5000)
      .config("spark.network.timeout", "200s")
      .config("spark.ui.enabled", "false")
      .config("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "AES256")
      .getOrCreate())
    val session = globalSession.newSession()
    session.conf.set("spark.app.name", appName)
    SparkSession.setActiveSession(session)
    session
  }

  "read Csv" should {
    "read a csv file" in {
      //GIVEN
      implicit val session: SparkSession = createSparkSession()
      val file = "src/test/resources/salaries-by-region.csv"

      //WHEN
      val result = Io.readCsv(file)

      //THEN
      result.count() shouldEqual 321
    }
  }

  "write Parquet" should {
    "write a dataframe to a parquet file" in {
      //GIVEN
      implicit val session: SparkSession = createSparkSession()
      import session.implicits._

      val path = "target/writeParquet"
      FileUtils.deleteDirectory(new File(path))

      val df = Seq(
        ("test1", 1, 900),
        ("test2", 2, 400))
        .toDF("name", "col2", "col3")

      //WHEN
      Io.writeParquet(df, path)

      //THEN
      Files.exists(Paths.get(path)) shouldBe true
      Files.exists(Paths.get(path + "/_SUCCESS")) shouldBe true
      session.read.parquet(path).count() should be(2)
    }
  }
}
