package au.com.mir

import org.apache.spark.sql.SparkSession
import au.com.mir.TestUtils.createSparkSession
import org.scalatest.WordSpec

class SparkJobTest extends WordSpec {

  "SparkJobTest" should {

    "join Denver_Crime and denver_offense codes" in {
      implicit val session: SparkSession = createSparkSession()
      import session.implicits._

      //GIVEN
      val denverCrime = Seq(
        (123, "INC123"),
        (456, "INC456"))
        .toDF("OFFENSE_CODE", "id")

      val offenseCodes = Seq(
        (123, "burglary"),
        (999, "break and enter"),
        (555, "assault"))
        .toDF("OFFENSE_CODE", "description")

      val expectedResult = Seq(
        (123, "INC123", "burglary"))
        .toDF("OFFENSE_CODE", "id", "description")

      //WHEN
      val result = SparkJob.execute(denverCrime, offenseCodes)

      //THEN
      result.equals(expectedResult)
    }

  }
}
