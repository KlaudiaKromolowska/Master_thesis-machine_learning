package com.plantarpressure.machineLearning

import com.plantarpressure.machineLearning.Classification.{changeFormat, splitData}
import com.plantarpressure.machineLearning.TrainModels.{decisionTree_classifier, decisionTree_classifier2, gradientBoosting_classifier, gradientBoosting_classifier2, kNearestNeighbors_classifier, logisticRegression_classifier, multilayerPerceptron_classifier, naiveBayes_classifier, randomForest_classifier, supportVectorMachine_classifier}
import org.apache.spark.ml.classification.{ClassificationModel, DecisionTreeClassifier, LogisticRegression}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.{Estimator, Pipeline}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, NaiveBayes}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DoubleType

object Classification {

  def checkCorrelation(data: DataFrame) = {
    val correlations = for {
      colName <- data.columns.filter(column => !column.toString.contains("C-") && column.toString != ("Diabetic"))
      corr = data.stat.corr("Diabetic", colName)
    } yield (colName, corr)
    println("Correlation between Diabetic and other columns:")
    println(Tabulator.formatTable(List("Column", "Correlation") +: correlations.sortBy(_._2.abs).reverse.map((x) => Seq(x._1, x._2.formatted("%.2f")))))
    println("Correlation between Diabetic and other groups of columns:")
    println(Tabulator.formatTable(List("Columns' group", "Correlation mean") +: correlations.groupBy(_._1.split("-")(0)).mapValues(x => (x.map(_._2.abs).sum / 3).formatted("%.2f")).toSeq.sortBy(_._2).reverse.map((x) => Seq(x._1, x._2))))
  }

  def splitData(data: DataFrame): (DataFrame, DataFrame) = {
    val Array(train, test) = data.randomSplit(Array(0.7, 0.3), 11L) //todo - maybe delete or change seed?
    train.cache() //to do - check if it's necessary
    (train, test)
  }

  def changeFormatToVector(data: DataFrame, columns: Array[String]): RDD[org.apache.spark.mllib.linalg.Vector] = {
    val assembler = new VectorAssembler()
      .setInputCols(columns)
      .setOutputCol("Values")
    val vectorData = assembler.transform(data)
    val rdd = vectorData.rdd.map(row => {
      val features = row.getAs[org.apache.spark.ml.linalg.SparseVector]("Values").toDense
      org.apache.spark.mllib.linalg.Vectors.fromML(features)
    })
    rdd
  }

  def changeFormat(data: DataFrame, columns: Array[String]): RDD[LabeledPoint] = {
    val assembler = new VectorAssembler()
      .setInputCols(columns)
      .setOutputCol("Values")
    val vectorData = assembler.transform(data)
    val rdd = vectorData.rdd.map(row => {
      val label = row.getAs[Double]("Diabetic")
      val features = row.getAs[org.apache.spark.ml.linalg.SparseVector]("Values").toDense
      LabeledPoint(label, org.apache.spark.mllib.linalg.Vectors.fromML(features))
    })
    rdd
  }


  def stratifiedSplit(data: DataFrame) = {
    val data1 = data.filter(data("Diabetic") === 1)
    val data0 = data.filter(data("Diabetic") === 0)
    val Array(train1, test1) = data1.randomSplit(Array(0.8, 0.2))
    val Array(train0, test0) = data0.randomSplit(Array(0.8, 0.2))
    val train = train1.union(train0)
    val test = test1.union(test0)
    train.cache()
    (train, test)
  }

  def summaryTable(data: DataFrame, columns: Array[String]): Unit = {

    val (training, testing) = stratifiedSplit(data)
    println("Diabetic column distribution - training set:")
    println(training.groupBy("Diabetic").count().show())
    println("Diabetic column distribution - testing set:")
    println(testing.groupBy("Diabetic").count().show())
    val trainingData = changeFormat(training, columns)
    val testingData = changeFormat(testing, columns)
    val trainingDataVector = changeFormatToVector(training, columns)
    val testingDataVector = changeFormatToVector(testing, columns)
    val eff_lrc = logisticRegression_classifier(trainingData, testingData)
    val eff_nbc = naiveBayes_classifier(trainingData, testingData)
    val eff_dct = decisionTree_classifier(trainingData, testingData)
    val eff_dct2 = decisionTree_classifier2(trainingData, testingData)
    val eff_rfc = randomForest_classifier(trainingData, testingData)
    val eff_svm = supportVectorMachine_classifier(trainingData, testingData)
    val eff_gbt = gradientBoosting_classifier(trainingData, testingData)
    val eff_gbt2 = gradientBoosting_classifier2(trainingData, testingData)
    val eff_knn = kNearestNeighbors_classifier(trainingDataVector, testingData)

    val table = Tabulator.formatTable(Seq(
      Seq("model", "Accuracy", "Precision", "Recall", "F1-Score"),
      Seq("Linear Regression Classifier") ++ eff_lrc.toSeq,
      Seq("Naive Bayes Classifier") ++ eff_nbc.toSeq,
      Seq("Decision Tree Classifier") ++ eff_dct.toSeq,
      Seq("Decision Tree Classifier 2") ++ eff_dct2.toSeq,
      Seq("Random Forest Classifier") ++ eff_rfc.toSeq,
      Seq("Support Vector Machine Classifier") ++ eff_svm.toSeq,
      Seq("Gradient Boosting Classifier") ++ eff_gbt.toSeq,
      Seq("Gradient Boosting Classifier 2") ++ eff_gbt2.toSeq,
      Seq("K Nearest Neighbors Classifier") ++ eff_knn.toSeq,
    ))
    println(table)
  }

  def effectivenessMeasureMulticlass(predictionAndLabels: RDD[(Double, Double)]) = {
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    val confusionMatrix = metrics.confusionMatrix
    println("Confusion matrix: \n" + confusionMatrix)
    val l = 1.0
    val precision = metrics.precision(l)
    val recall = metrics.recall(l)
    val f1Score = metrics.fMeasure(l)
    Array(accuracy, precision, recall, f1Score)

  }

}
