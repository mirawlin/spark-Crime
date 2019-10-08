package au.com.mir

import au.com.mir.io.Io
import org.apache.spark.sql.SparkSession

object Main extends App {
  implicit val session: SparkSession = SparkSupport.getOrCreateSession()

  val df = Io.readCsv("src/main/resources/LA_MO_Codes.csv")

  Io.writeParquet(df, "target/Codes")

}

object SparkSupport {

  def createSession(appName: String = getClass.getName): SparkSession = {
    SparkSession.clearActiveSession()
    val globalSession = SparkSession.getDefaultSession.getOrElse(SparkSession.builder()
      .appName("Global session")
      .master("local[2]")
      .getOrCreate())
    val session = globalSession.newSession()
    session.conf.set("spark.app.name", appName)
    SparkSession.setActiveSession(session)
    session
  }

  def getOrCreateSession(appName: String = getClass.getName): SparkSession = SparkSession
    .getActiveSession
    .getOrElse(createSession(appName))
}
