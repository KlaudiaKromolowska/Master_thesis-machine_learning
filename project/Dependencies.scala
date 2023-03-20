import sbt._
object Dependencies {
  import Versions._
  val all = Seq(
    "org.scalatest" %% "scalatest" % scalaTest % Test,
    "org.apache.spark" %% "spark-core" % spark,
    "org.apache.spark" %% "spark-sql" % spark,
    "com.crealytics" %% "spark-excel" % sparkExcel,
    "org.apache.logging.log4j" % "log4j-core" % log4j,
    "org.apache.spark" %% "spark-mllib" % spark,
  "com.github.aneureka" % "PrettyTable4J" % prettyTable
  )
}

object Versions {
  val scalaTest = "3.2.11"
  val spark = "3.2.1"
  val sparkExcel =  "3.2.2_0.18.0"
  val log4j = "2.17.1"
  val prettyTable = "0.0.2"
}
