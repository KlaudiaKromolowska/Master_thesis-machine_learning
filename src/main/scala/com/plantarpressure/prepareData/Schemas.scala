package com.plantarpressure.prepareData

import org.apache.spark.sql.types._

object Schemas {

  val schemaInit = StructType(
    Array(
      StructField("ID", IntegerType, nullable = false),
      StructField("Time", StringType, nullable = false),
      StructField("Date", StringType, nullable = false),
      StructField("MTK1-T", DoubleType, nullable = false),
      StructField("MTK2-T", DoubleType, nullable = false),
      StructField("MTK3-T", DoubleType, nullable = false),
      StructField("MTK4-T", DoubleType, nullable = false),
      StructField("MTK5-T", DoubleType, nullable = false),
      StructField("D1-T", DoubleType, nullable = false),
      StructField("L-T", DoubleType, nullable = false),
      StructField("C-T", DoubleType, nullable = false),
      StructField("Env-T", DoubleType, nullable = false),
      StructField("MTK1-P", DoubleType, nullable = false),
      StructField("MTK2-P", DoubleType, nullable = false),
      StructField("MTK3-P", DoubleType, nullable = false),
      StructField("MTK4-P", DoubleType, nullable = false),
      StructField("MTK5-P", DoubleType, nullable = false),
      StructField("D1-P", DoubleType, nullable = false),
      StructField("L-P", DoubleType, nullable = false),
      StructField("C-P", DoubleType, nullable = false)
    ))
  val schemaInit_withoutTime = StructType(
    Array(
      StructField("MTK1-T", DoubleType, nullable = false),
      StructField("MTK2-T", DoubleType, nullable = false),
      StructField("MTK3-T", DoubleType, nullable = false),
      StructField("MTK4-T", DoubleType, nullable = false),
      StructField("MTK5-T", DoubleType, nullable = false),
      StructField("D1-T", DoubleType, nullable = false),
      StructField("L-T", DoubleType, nullable = false),
      StructField("C-T", DoubleType, nullable = false),
      StructField("Env-T", DoubleType, nullable = false),
      StructField("MTK1-P", DoubleType, nullable = false),
      StructField("MTK2-P", DoubleType, nullable = false),
      StructField("MTK3-P", DoubleType, nullable = false),
      StructField("MTK4-P", DoubleType, nullable = false),
      StructField("MTK5-P", DoubleType, nullable = false),
      StructField("D1-P", DoubleType, nullable = false),
      StructField("L-P", DoubleType, nullable = false),
      StructField("C-P", DoubleType, nullable = false)
    ))

  val schemaMeanMinMax = StructType(
    Array(
      StructField("MTK1-mean", DoubleType, nullable = false),
      StructField("MTK2-mean", DoubleType, nullable = false),
      StructField("MTK3-mean", DoubleType, nullable = false),
      StructField("MTK4-mean", DoubleType, nullable = false),
      StructField("MTK5-mean", DoubleType, nullable = false),
      StructField("D1-mean", DoubleType, nullable = false),
      StructField("L-mean", DoubleType, nullable = false),
      StructField("C-mean", DoubleType, nullable = false),

      StructField("MTK1-min", DoubleType, nullable = false),
      StructField("MTK2-min", DoubleType, nullable = false),
      StructField("MTK3-min", DoubleType, nullable = false),
      StructField("MTK4-min", DoubleType, nullable = false),
      StructField("MTK5-min", DoubleType, nullable = false),
      StructField("D1-min", DoubleType, nullable = false),
      StructField("L-min", DoubleType, nullable = false),
      StructField("C-min", DoubleType, nullable = false),

      StructField("MTK1-max", DoubleType, nullable = false),
      StructField("MTK2-max", DoubleType, nullable = false),
      StructField("MTK3-max", DoubleType, nullable = false),
      StructField("MTK4-max", DoubleType, nullable = false),
      StructField("MTK5-max", DoubleType, nullable = false),
      StructField("D1-max", DoubleType, nullable = false),
      StructField("L-max", DoubleType, nullable = false),
      StructField("C-max", DoubleType, nullable = false),

      StructField("Toes-mean", DoubleType, nullable = false),
      StructField("Toes-min", DoubleType, nullable = false),
      StructField("Toes-max", DoubleType, nullable = false),

      StructField("Foot-mean", DoubleType, nullable = false),
      StructField("Foot-min", DoubleType, nullable = false),
      StructField("Foot-max", DoubleType, nullable = false),

      StructField("Diabetic", DoubleType, nullable = false)
    ))

  val schemaTime = StructType(
    Array(
      StructField("Time", StringType, nullable = false),

      StructField("MTK1-mean", DoubleType, nullable = false),
      StructField("MTK2-mean", DoubleType, nullable = false),
      StructField("MTK3-mean", DoubleType, nullable = false),
      StructField("MTK4-mean", DoubleType, nullable = false),
      StructField("MTK5-mean", DoubleType, nullable = false),
      StructField("D1-mean", DoubleType, nullable = false),
      StructField("L-mean", DoubleType, nullable = false),
      StructField("C-mean", DoubleType, nullable = false),
      StructField("Env-mean", DoubleType, nullable = false),

      StructField("MTK1-min", DoubleType, nullable = false),
      StructField("MTK2-min", DoubleType, nullable = false),
      StructField("MTK3-min", DoubleType, nullable = false),
      StructField("MTK4-min", DoubleType, nullable = false),
      StructField("MTK5-min", DoubleType, nullable = false),
      StructField("D1-min", DoubleType, nullable = false),
      StructField("L-min", DoubleType, nullable = false),
      StructField("C-min", DoubleType, nullable = false),
      StructField("Env-min", DoubleType, nullable = false),

      StructField("MTK1-max", DoubleType, nullable = false),
      StructField("MTK2-max", DoubleType, nullable = false),
      StructField("MTK3-max", DoubleType, nullable = false),
      StructField("MTK4-max", DoubleType, nullable = false),
      StructField("MTK5-max", DoubleType, nullable = false),
      StructField("D1-max", DoubleType, nullable = false),
      StructField("L-max", DoubleType, nullable = false),
      StructField("C-max", DoubleType, nullable = false),
      StructField("Env-max", DoubleType, nullable = false),

      StructField("Toes-mean", DoubleType, nullable = false),
      StructField("Toes-min", DoubleType, nullable = false),
      StructField("Toes-max", DoubleType, nullable = false),

      StructField("Foot-mean", DoubleType, nullable = false),
      StructField("Foot-min", DoubleType, nullable = false),
      StructField("Foot-max", DoubleType, nullable = false),

    ))
}
