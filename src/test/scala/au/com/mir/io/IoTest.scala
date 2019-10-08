package au.com.mir.io

import java.io.File
import java.nio.file.{ Files, Paths }

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import au.com.mir.TestUtils.createSparkSession
import org.scalatest.{ Matchers, WordSpec }

class IoTest extends WordSpec with Matchers {

  "read Csv" should {
    "read a csv file" in {
      //GIVEN
      implicit val session: SparkSession = createSparkSession()

      val file = "src/test/resources/boston_crime.csv"
      val file2 = "src/test/resources/crime_denver.csv"
      val file3 = "src/test/resources/Crime_in_LA_Data_2010_2017.csv"


      //WHEN
      val result = Io.readCsv(file)
      result.show(10)

      val result2 = Io.readCsv(file2)
      result2.show(10)

      val result3 = Io.readCsv(file3)
      result3.show(10)

      //THEN
      result.count() shouldEqual 327820

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
