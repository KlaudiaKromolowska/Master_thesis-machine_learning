package com.plantarpressure.prepareData

import com.plantarpressure.configuration.Config.{pressure_path, temperature_path}
import com.plantarpressure.prepareData.Import.importFiles
import com.plantarpressure.prepareData.PrepareDataFrames.prepareDataFrames
import com.plantarpressure.prepareData.Schemas.schemaMeanMinMax
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.annotation.tailrec

object PrepareFiles {

  def createOrcFiles(spark: SparkSession): Unit = {
    val (dataControl: Seq[sql.DataFrame], dataDiabetics: Seq[sql.DataFrame]) = importFiles()
    val (dfTemp, dfPressure) = createDataFrames(dataControl.map(_.drop("Env-T")), dataDiabetics.map(_.drop("Env-T")), spark)
    dfTemp.write.orc(temperature_path)
    dfPressure.write.orc(pressure_path)
  }

  def createDataFrames(dataControl: Seq[sql.DataFrame], dataDiabetics: Seq[sql.DataFrame], spark: SparkSession): (DataFrame, DataFrame) = {

    def createListWithAllData(isDiabetic: Double, data: Seq[sql.DataFrame]): (List[Row], List[Row]) = {

      @tailrec
      def tailrecListWithAllData(isDiabetic: Double, data: Seq[sql.DataFrame], lists: (List[Row], List[Row])): (List[Row], List[Row]) = {
        if (data.isEmpty) lists
        else {
          val (listTemp, listPressure) = prepareDataFrames(data.head, isDiabetic, spark)
          println("Size of data: " + data.size + "lists: " + lists._1)
          tailrecListWithAllData(isDiabetic, data.tail,
            (lists._1.::(Row.fromSeq(listTemp)), lists._2.::(Row.fromSeq(listPressure)))
          )
        }
      }

      tailrecListWithAllData(isDiabetic, data, (List(), List()))
    }

    import scala.collection.JavaConverters._
    val listOfRowsNonDiabetics = createListWithAllData(0, dataControl)
    val listOfRowsDiabetics = createListWithAllData(1, dataDiabetics)
    val allRowsTemp: java.util.List[Row] = (listOfRowsNonDiabetics._1).union(listOfRowsDiabetics._1).asJava
    val allRowsPressure: java.util.List[Row] = (listOfRowsNonDiabetics._2).union(listOfRowsDiabetics._2).asJava
    (spark.createDataFrame(allRowsTemp, schemaMeanMinMax), spark.createDataFrame(allRowsPressure, schemaMeanMinMax))
  }


}
