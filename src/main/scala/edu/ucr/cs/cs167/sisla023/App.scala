package edu.ucr.cs.cs167.sisla023

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object App {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("Chicago Crime Arrest Prediction")
      .getOrCreate()

    // Load the pre-processed Chicago Crime dataset in Parquet format.
    // This dataset is assumed to have been cleaned in Tasks 1-4 and includes columns such as:
    // "PrimaryType" (crime type), "Arrest" (whether an arrest was made), "District", etc.
    val crimeData = spark.read.parquet("path/to/chicago_crime_prepared.parquet")

    // --- Data Preparation ---
    // Convert the "PrimaryType" (categorical) into a numerical index.
    val primaryTypeIndexer = new StringIndexer()
      .setInputCol("PrimaryType")
      .setOutputCol("PrimaryTypeIndex")
      .setHandleInvalid("keep")

    // Convert the "Arrest" column (which is a string like "TRUE"/"FALSE" or a boolean)
    // into a numeric label (0.0/1.0).
    val arrestIndexer = new StringIndexer()
      .setInputCol("Arrest")
      .setOutputCol("label")
      .setHandleInvalid("keep")

    // Assemble features into a single vector.
    // Here we include the indexed crime type and the district.
    // You can add more features if available (e.g., extracting the hour from the crime time).
    val assembler = new VectorAssembler()
      .setInputCols(Array("PrimaryTypeIndex", "District"))
      .setOutputCol("features")

    // --- Model Training ---
    // Create a logistic regression classifier for binary prediction.
    val logisticRegression = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxIter(10)

    // Build the pipeline with all the stages.
    val pipeline = new Pipeline().setStages(Array(primaryTypeIndexer, arrestIndexer, assembler, logisticRegression))

    // Split the dataset into training (80%) and test (20%) sets.
    val Array(trainingData, testData) = crimeData.randomSplit(Array(0.8, 0.2), seed = 12345)

    // Fit the pipeline to the training data.
    val model = pipeline.fit(trainingData)

    // --- Predictions and Evaluation ---
    // Use the trained model to make predictions on the test data.
    val predictions = model.transform(testData)
    predictions.select("features", "label", "prediction").show(5)

    // Evaluate the model using the area under the ROC curve.
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")

    val auc = evaluator.evaluate(predictions)
    println(s"Test Area Under ROC: $auc")

    // Optionally, save the trained model for later use.
    model.write.overwrite().save("path/to/save/chicago_crime_arrest_model")

    spark.stop()
  }
}