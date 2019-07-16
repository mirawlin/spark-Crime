name := "spark-SalariesAfterCollege"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq("org.specs2" %% "specs2-core" % "3.8.6" % "test")
libraryDependencies += "org.specs2" % "specs2-junit_2.11" % "3.8.6" % "test"
libraryDependencies += "org.specs2" % "specs2-scalacheck_2.11" % "3.8.6" % "test"
libraryDependencies += "org.specs2" % "specs2-matcher-extra_2.11" % "3.8.6" % "test"
libraryDependencies += "org.specs2" % "specs2-mock_2.11" % "3.8.6" % "test"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.0"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.25" % "test"

parallelExecution in Test := false
