package com.plantarpressure.configuration

object Config {
  def setProperties(): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\PC\\Downloads\\hadoop-2.8.1.tar\\hadoop-2.8.1")
  }

  val appName = "plantarPressureAnalysis"

  val MEAN_POS = 1
  val STD_POS = 2
  val MIN_POS = 3
  val MAX_POS = 7
  val temperature_path = "src/main/resources/orc/temperature.orc"
  val pressure_path = "src/main/resources/orc/pressure.orc"
}
