package edu.ucr.cs.cs167.master

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, StringIndexer, Tokenizer}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TaskA5 {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: TaskA5 <input file>")
      System.exit(1)
    }

    val inputFile = args(0)

    val spark = SparkSession.builder
      .appName("Chicago Crime Arrest Prediction")
      .master("local[*]")
      .getOrCreate()

    // Reading the parquet files into the dataframe
    val df = spark.read.parquet(inputFile)
    // Creating a temp view to use spark sql on dataset
    df.createOrReplaceTempView("crime")

    // query to retrieve only records where Arrest is 'true' or 'false'
    val filteredDF = spark.sql("SELECT * FROM crime WHERE Arrest IN ('true', 'false')")

    // Combine PrimaryType and Description into one column to make tokenization easier
    val combinedDF = filteredDF.withColumn("combinedText", concat_ws(" ", col("PrimaryType"), col("Description")))

    // ML Pipeline:
    // 1. Tokenizer
    val tokenizer = new Tokenizer()
      .setInputCol("combinedText")
      .setOutputCol("tokens")

    // 2. HashingTF
    val hashingTF = new HashingTF()
      .setInputCol("tokens")
      .setOutputCol("features")
      .setNumFeatures(1000)

    // 3. StringIndexer
    val arrestIndexer = new StringIndexer()
      .setInputCol("Arrest")
      .setOutputCol("label")
      .setHandleInvalid("keep")

    // 4. Logistic Regression classifier
    val lr = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxIter(10)

    // Building the ML pipeline
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, arrestIndexer, lr))

    // Split the dataset into training (80%) and test (20%) sets.
    val Array(trainingData, testData) = combinedDF.randomSplit(Array(0.8, 0.2), seed = 12345)

    // Start time to record timer spent running the model
    val startTime = System.nanoTime()

    // Training the model
    val model = pipeline.fit(trainingData)

    // end time and compute total training time.
    val endTime = System.nanoTime()
    val totalTimeSeconds = (endTime - startTime) / 1e9d

    // Apply model to test data
    val predictions = model.transform(testData)
    predictions.select("PrimaryType", "Description", "Arrest", "label", "prediction").show(10, truncate = false)

    // Evaluate precision and recall.
    import spark.implicits._
    val predictionAndLabels = predictions.select("prediction", "label")
      .as[(Double, Double)]
      .rdd // Convert predictions to an RDD of (prediction, label) tuples.

    val metrics = new MulticlassMetrics(predictionAndLabels)
    // For binary classification, label 1.0 indicates a "true" arrest.
    val precision = metrics.precision(1.0)
    val recall = metrics.recall(1.0)

    println(s"Total training time: $totalTimeSeconds seconds")
    println(s"Precision: $precision")
    println(s"Recall: $recall")

    spark.stop()
  }
}
