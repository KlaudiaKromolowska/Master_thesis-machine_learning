package com.plantarpressure.prepareData

import com.plantarpressure.configuration.Config
import com.plantarpressure.configuration.Config.{MAX_POS, MEAN_POS, MIN_POS, STD_POS}
import com.plantarpressure.prepareData.Schemas.{schemaInit, schemaInit_withoutTime}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.jdk.CollectionConverters.seqAsJavaListConverter

object PrepareDataFrames extends App {

  Config.setProperties()

  def prepareDataFrames(dataFrame: DataFrame, diabeticsOrNot: Double, spark: SparkSession): (Seq[Double], Seq[Double]) = {
    val dataFrameTemp = prepareDataFrameSummary(dataFrame, "-T", diabeticsOrNot, spark)
    val dataFramePressure = prepareDataFrameSummary(squareRootFromDataframe(dataFrame, spark), "-P", diabeticsOrNot, spark)
    (dataFrameTemp, dataFramePressure)
  }


  def squareRootFromDataframe(dataFrame: DataFrame, spark: SparkSession): DataFrame = {
    val maxValue = for {
      column <- dataFrame.columns.filterNot(_ == "Diabetic").filterNot(_ == "Time").filterNot(_ == "ID").filterNot(_ == "Date")
      maxValue = dataFrame.select(column).rdd.map { rows =>
        val maxFromRow: Seq[Double] = for {
          rowNumber <- 0 until rows.length
          values = rows.get(rowNumber).toString.toDouble
        } yield values
        maxFromRow.max
      }.max()
    } yield maxValue
    val max = maxValue.max
    if (max > 4096) {
      val rows = dataFrame.rdd.map(_.toSeq.map {
        case x: Double if x > 1 => (x / 8).toDouble
        case x: Double if x <= 0 => 0.toDouble
        case x => x
      }).map(Row.fromSeq)
      spark.createDataFrame(rows, dataFrame.schema)
    }
    else dataFrame
  }

  def prepareDataFrameSummary(dataFrame: DataFrame, s: String, diabeticsOrNot: Double, spark: SparkSession): Seq[Double] = { //s would be -T or -P
    val rows = dataFrame.rdd.collect()

    val summaryDataFrame = dataFrame.summary()

    val summaryRows = summaryDataFrame.rdd.collect()
    val meanRow = summaryRows(MEAN_POS).toSeq.drop(4)
    val stdRow = summaryRows(STD_POS).toSeq.drop(4)
    val sigmaRowWithNegative = (meanRow, stdRow).zipped.map((a, b) => a.toString.toDouble + 3 * b.toString.toDouble)
    val sigmaRow = sigmaRowWithNegative.map(_.toString.toDouble).map(x => if (x < 0) 0 else x)

    val filteredRows: Array[Row] = {
      if (s == "-T") {
        val filteredRows = rows.filter { row =>
          val rowWithoutNegativeValues = row.toSeq.drop(3).map(_.toString.toDouble).map(x => if (x < 0) 0 else x)
          rowWithoutNegativeValues.map(_.toString.toDouble)
          (rowWithoutNegativeValues, sigmaRow).zipped.forall(_ <= _)
        }
        for {
          row <- filteredRows
        } yield Row.fromSeq(row.toSeq.drop(3))
      } else for {
        row <- rows
        rowWithoutNegativeValues = row.toSeq.drop(3).map(_.toString.toDouble).map(x => if (x < 0) 0 else x)
      } yield Row.fromSeq(rowWithoutNegativeValues :+ 0.0)
    }

    val filteredRowsList: java.util.List[Row] = filteredRows.toList.asJava
    val filteredDataFrame: DataFrame = (spark.createDataFrame(filteredRowsList, schemaInit_withoutTime))

    val newDataFrame = filteredDataFrame.select(s"MTK1$s", s"MTK2$s", s"MTK3$s", s"MTK4$s", s"MTK5$s", s"D1$s", s"L$s", s"C$s").summary()


    createRowPerMeasure(newDataFrame, s, diabeticsOrNot)
  }


  def createRowPerMeasure(dataFrame: DataFrame, s: String, diabeticsOrNot: Double = -1.0): Seq[Double] = {

    val rows = dataFrame.rdd.collect()
    val meanDataFrame = dataFrame.selectExpr(
      s"(`D1$s` + `MTK1$s` + `MTK2$s` + `MTK3$s` + `MTK4$s` + `MTK5$s` + `L$s` + `C$s`)/8 as `MeanToes$s`",
      s"(`L$s` + `C$s` + `D1$s` + `MTK1$s` + `MTK2$s` + `MTK3$s` + `MTK4$s` + `MTK5$s` + `L$s` + `C$s`)/10 as `MeanFoot$s`")


    val dataMeanMinMaxPerSensor: Seq[Double] = (rows(MEAN_POS).toSeq.drop(1) ++ rows(MIN_POS).toSeq.drop(1) ++ rows(MAX_POS).toSeq.drop(1)).map(_.toString.toDouble)
    val dataMeanMinMaxToes: Seq[Double] = createSeqOfValues(meanDataFrame, s"MeanToes$s")
    val dataMeanMinMaxFoot: Seq[Double] = createSeqOfValues(meanDataFrame, s"MeanFoot$s")
    if (diabeticsOrNot == -1) dataMeanMinMaxPerSensor ++ dataMeanMinMaxToes ++ dataMeanMinMaxFoot
    else dataMeanMinMaxPerSensor ++ dataMeanMinMaxToes ++ dataMeanMinMaxFoot :+ diabeticsOrNot.toDouble
  }

  def createSeqOfValues(df: DataFrame, column: String): Seq[Double] = {
    val arrayOfRows = df.select(column).rdd.collect()
    (arrayOfRows(MEAN_POS).toSeq ++ arrayOfRows(MIN_POS).toSeq ++ arrayOfRows(MAX_POS).toSeq).map(_.toString.toDouble)
  }
}
