package com.plantarpressure.machineLearning

object Tabulator {
  def formatTable(table: Seq[Seq[Any]]): String = {
    if (table.isEmpty) ""
    else {
      val colWidths = table.transpose.map(_.map(cell => if (cell == null) 0 else cell.toString.length).max + 2)
      val rows = table.map(_.zip(colWidths).map { case (item, size) => (" %-" + (size - 1) + "s").format(item) }
        .mkString("|", "|", "|"))
      val separator = colWidths.map("-" * _).mkString("+", "+", "+")
      (separator +: rows.head +: separator +: rows.tail :+ separator).mkString("\n")
    }
  }
}
