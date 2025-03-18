package edu.ucr.cs.cs167.master

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Scala program for Temporal Crime Analysis using Beast and Spark
 */
object CrimeTemporalAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val conf = new SparkConf().setAppName("Crime Temporal Analysis")

    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    // Validate command-line arguments
    if (args.length < 2) {
      println("Argument Usage: <start_date> <end_date>")
      sys.exit(1)
    }

    // Parse start and end dates from command-line arguments
    val dataFile = args(0)   // Expected format: parquet file
    val startDate = args(1)  // Expected format: MM/DD/YYYY
    val endDate = args(2)    // Expected format: MM/DD/YYYY

    try {
      // Load crime dataset from Parquet format
      val crimeDF = spark.read.parquet(dataFile)

      // Register DataFrame as a temporary SQL table
      crimeDF.createOrReplaceTempView("crimes")

      // Run SQL query for temporal analysis
      val query =
        s"""
           |SELECT PrimaryType, COUNT(*) as count
           |FROM (
           |  SELECT PrimaryType,
           |         to_timestamp(Date, 'MM/dd/yyyy hh:mm:ss a') AS CrimeTimestamp
           |  FROM crimes
           |)
           |WHERE to_date(CrimeTimestamp, 'MM/dd/yyyy')
           |      BETWEEN to_date('$startDate', 'MM/dd/yyyy') AND to_date('$endDate', 'MM/dd/yyyy')
           |GROUP BY PrimaryType
         """.stripMargin

      // Execute query
      val crimeCountsDF = spark.sql(query)

      // Save results as CSV
      crimeCountsDF.coalesce(1)
        .write
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv("CrimeTypeCount")

      println("CrimeTypeCount.csv has been successfully generated!")

    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
