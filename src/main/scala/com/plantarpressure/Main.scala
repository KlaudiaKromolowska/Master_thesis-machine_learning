package com.plantarpressure

import com.plantarpressure.configuration.Config
import com.plantarpressure.configuration.Config.{appName, pressure_path, temperature_path}
import com.plantarpressure.machineLearning.Classification.{checkCorrelation, summaryTable}
import com.plantarpressure.prepareData.PrepareFiles.createOrcFiles
import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

object Main extends App {

  Config.setProperties()

  val spark: SparkSession = SparkSession.builder
    .appName(appName)
    .master("local")
    .config("spark.driver.memory", "15g")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //ANALIZE DATA
  if(!File(temperature_path).exists || !File(pressure_path).exists) createOrcFiles(spark)
  private val pressureDataFrame = spark.read.orc(pressure_path)
  val tempDataFrame = spark.read.orc(temperature_path)

  print("PRESSURE DATAFRAME: ")
  pressureDataFrame.show(10)
  pressureDataFrame.summary().show()

  val columnsAll = Array("MTK1-mean", "MTK2-mean", "MTK3-mean", "MTK4-mean", "MTK5-mean", "D1-mean", "L-mean", "C-mean", "MTK1-min", "MTK2-min", "MTK3-min", "MTK4-min", "MTK5-min", "D1-min", "L-min", "C-min", "MTK1-max", "MTK2-max", "MTK3-max", "MTK4-max", "MTK5-max", "D1-max", "L-max", "C-max", "Toes-min", "Toes-mean", "Toes-max", "Foot-mean", "Foot-min", "Foot-max")
  val columnsWithoutSummary = Array("MTK1-mean", "MTK2-mean", "MTK3-mean", "MTK4-mean", "MTK5-mean", "D1-mean", "L-mean", "C-mean", "MTK1-min", "MTK2-min", "MTK3-min", "MTK4-min", "MTK5-min", "D1-min", "L-min", "C-min", "MTK1-max", "MTK2-max", "MTK3-max", "MTK4-max", "MTK5-max", "D1-max", "L-max", "C-max")
  val columnsWithCorrelationsPressure = Array("D1-max", "D1-mean", "L-min", "L-mean", "MTK2-min")
  val columnsWithGroupCorrelationsPressure = Array("L-mean", "L-min", "L-max", "D1-mean", "D1-min", "D1-max", "MTK4-min", "MTK4-mean", "MTK4-max")
  println("PRESSURE - CORRELATIONS: ")
  checkCorrelation(pressureDataFrame)
  println("PRESSURE - ALL COLUMNS: ")
  summaryTable(pressureDataFrame, columnsAll)
  println("PRESSURE - WITHOUT SUMMARY COLUMNS: ")
  summaryTable(pressureDataFrame, columnsWithoutSummary)
  println("PRESSURE - WITH CORRELATIONS COLUMNS: ")
  summaryTable(pressureDataFrame, columnsWithCorrelationsPressure)
  println("PRESSURE - WITH GROUP CORRELATIONS COLUMNS: ")
  summaryTable(pressureDataFrame, columnsWithGroupCorrelationsPressure)


  val columnsWithCorrelationsTemp = Array("Foot-mean", "Foot-max", "Foot-min", "L-max")
  val columnsWithGroupCorrelationsTemp = Array("Foot-mean", "Foot-min", "Foot-max", "L-mean", "L-min", "L-max", "Toes-min", "Toes-mean", "Toes-max")
  print("TEMPERATURE DATAFRAME: ")
  tempDataFrame.show(10)
  tempDataFrame.summary().show()

  println("TEMPERATURE - CORRELATIONS: ")
  checkCorrelation(tempDataFrame)
  println("TEMPERATURE - ALL COLUMNS: ")
  summaryTable(tempDataFrame, columnsAll)
  println("TEMPERATURE - WITHOUT SUMMARY COLUMNS: ")
  summaryTable(tempDataFrame, columnsWithoutSummary)
  println("TEMPERATURE - WITH CORRELATIONS COLUMNS: ")
  summaryTable(tempDataFrame, columnsWithCorrelationsTemp)
  println("TEMPERATURE - WITH GROUP CORRELATIONS COLUMNS: ")
  summaryTable(tempDataFrame, columnsWithGroupCorrelationsTemp)
}
