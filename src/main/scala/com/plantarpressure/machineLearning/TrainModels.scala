package com.plantarpressure.machineLearning

import com.plantarpressure.configuration.Config.appName
import com.plantarpressure.machineLearning.Classification.effectivenessMeasureMulticlass
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, NaiveBayes, SVMWithSGD}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.{DecisionTree, GradientBoostedTrees}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TrainModels {
  val spark: SparkSession = SparkSession.builder
    .appName(appName)
    .master("local")
    .config("spark.driver.memory", "15g")
    .getOrCreate()

  def logisticRegression_classifier(train: RDD[LabeledPoint], test: RDD[LabeledPoint]): Array[Double] = {
    val logisticRegressionWithLBFGS = new LogisticRegressionWithLBFGS().setNumClasses(2).run(train)
    val scoreAndLabels = test.map { point =>
      (logisticRegressionWithLBFGS.predict(point.features), point.label)
    }
    effectivenessMeasureMulticlass(scoreAndLabels)
  }

  def naiveBayes_classifier(train: RDD[LabeledPoint], test: RDD[LabeledPoint]): Array[Double] = {
    val train2 = train.map { point =>
      val features = point.features.toArray
      val features2 = features.map { x =>
        if (x < 0) 0.0 else x
      }
      LabeledPoint(point.label, org.apache.spark.mllib.linalg.Vectors.dense(features2))
    }
    val naiveBayes = NaiveBayes.train(train2, lambda = 1.0, modelType = "multinomial")
    val scoreAndLabels = test.map { point =>
      (naiveBayes.predict(point.features), point.label)
    }
    effectivenessMeasureMulticlass(scoreAndLabels)
  }

  def decisionTree_classifier(train: RDD[LabeledPoint], test: RDD[LabeledPoint]): Array[Double] = {
    val decisionTree = DecisionTree.trainClassifier(train, 2, Map[Int, Int](), "gini", 5, 32)
    val scoreAndLabels = test.map { point =>
      (decisionTree.predict(point.features), point.label)
    }
    effectivenessMeasureMulticlass(scoreAndLabels)
  }

  def decisionTree_classifier2(train: RDD[LabeledPoint], test: RDD[LabeledPoint]): Array[Double] = {
    val decisionTree = DecisionTree.trainClassifier(train, 2, Map[Int, Int](), "entropy", 10, 32)
    val scoreAndLabels = test.map { point =>
      (decisionTree.predict(point.features), point.label)
    }
    effectivenessMeasureMulticlass(scoreAndLabels)
  }


  def gradientBoosting_classifier(train: RDD[LabeledPoint], test: RDD[LabeledPoint]): Array[Double] = {
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 5
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(train, boostingStrategy)
    val scoreAndLabels = test.map { point =>
      (model.predict(point.features), point.label)
    }
    effectivenessMeasureMulticlass(scoreAndLabels)
  }

  def gradientBoosting_classifier2(train: RDD[LabeledPoint], test: RDD[LabeledPoint]): Array[Double] = {
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 20 // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 10
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
    boostingStrategy.treeStrategy.maxBins = 32
    boostingStrategy.learningRate = 0.1

    val model = GradientBoostedTrees.train(train, boostingStrategy)
    val scoreAndLabels = test.map { point =>
      (model.predict(point.features), point.label)
    }
    effectivenessMeasureMulticlass(scoreAndLabels)
  }

  def randomForest_classifier(train: RDD[LabeledPoint], test: RDD[LabeledPoint]): Array[Double] = {
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(train, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    val scoreAndLabels = test.map { point =>
      (model.predict(point.features), point.label)
    }
    effectivenessMeasureMulticlass(scoreAndLabels)
  }

  def multilayerPerceptron_classifier(train: RDD[LabeledPoint], test: RDD[LabeledPoint]): Array[Double] = {
    val trainDataframe = spark.createDataFrame(train)
    val layers = Array[Int](4, 5, 4, 2)
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)
    val model = trainer.fit(trainDataframe)
    val result = model.transform(trainDataframe)
    val predictionAndLabels = result.select("prediction", "label")
    val scoreAndLabels = predictionAndLabels.rdd.map(row => (row.getDouble(0), row.getDouble(1)))
    effectivenessMeasureMulticlass(scoreAndLabels)
  }

  def supportVectorMachine_classifier(train: RDD[LabeledPoint], test: RDD[LabeledPoint]): Array[Double] = {
    val svm = SVMWithSGD.train(train, 100)
    val scoreAndLabels = test.map { point =>
      (svm.predict(point.features), point.label)
    }
    effectivenessMeasureMulticlass(scoreAndLabels)
  }

  def kNearestNeighbors_classifier(train: RDD[Vector], test: RDD[LabeledPoint]): Array[Double] = {
    val model = KMeans.train(train, k = 1, maxIterations = 100)
    val scoreAndLabels = test.map { point =>
      (model.predict(point.features).toDouble, point.label)
    }
    effectivenessMeasureMulticlass(scoreAndLabels)
  }


}
