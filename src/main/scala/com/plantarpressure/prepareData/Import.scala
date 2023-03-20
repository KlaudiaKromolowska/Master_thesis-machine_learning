package com.plantarpressure.prepareData

import com.plantarpressure.Main.spark.sqlContext
import com.plantarpressure.prepareData.Schemas.schemaInit
import org.apache.spark.sql.{Column, DataFrame}

import java.io.File

object Import {

  def importFiles(): (List[DataFrame], List[DataFrame]) = {
    val pathDiabetics = "src/main/resources/datasets/diabetics"
    val pathControlGroup = "src/main/resources/datasets/control_group"
    (importFilesFromPath(pathControlGroup), importFilesFromPath(pathDiabetics))
  }

  def importFilesFromPath(path: String): List[DataFrame] = {
    val dataFrame: Seq[DataFrame] = for {
      file <- getListOfFiles(path)
      dataFrame = sqlContext.read
        .format("com.crealytics.spark.excel")
        .option("header", true)
        .schema(schemaInit)
        .load(file.getPath)
    } yield dataFrame
    dataFrame.toList
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles().filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}
