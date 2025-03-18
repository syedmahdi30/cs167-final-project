package edu.ucr.cs.cs167.master

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TaskA4 {

  def main(args: Array[String]): Unit = {

    if (args.length != 7) {
      println("Invalid arguments provided. Please provide the correct arguments:")
      println("Usage: <dataFile> <start_date> <end_date> <x_min> <y_min> <x_max> <y_max>")
    }

    val dataFile: String = args(0)
    val start_date: String = args(1)
    val end_date: String = args(2)

    val x_min: Double = args(3).toDouble
    val y_min: Double = args(4).toDouble
    val x_max: Double = args(5).toDouble
    val y_max: Double = args(6).toDouble

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CrimeRangeReport")
      .config(conf)
      .getOrCreate()

    try {
      val dataFileDF = spark.read.parquet(dataFile)

      // Parse the date column into a proper format
      val parsedDate = dataFileDF
        .withColumn("CrimeTimestamp", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))
        .withColumn("FormattedCrimeTimestamp", date_format(col("CrimeTimestamp"), "MM/dd/yyyy hh:mm:ss a"))

      parsedDate.createOrReplaceTempView("crimeData")

      // SQL query to filter crimes within the date range and spatial boundaries
      val resultDF =
        spark.sql(
          s"""
           SELECT X, Y, CaseNumber, FormattedCrimeTimestamp AS CrimeTimestamp
           FROM crimeData
           WHERE CrimeTimestamp BETWEEN to_date('$start_date', 'MM/dd/yyyy')
                               AND to_date('$end_date', 'MM/dd/yyyy')
             AND X BETWEEN $x_min AND $x_max
             AND Y BETWEEN $y_min AND $y_max
         """)

      // Save output as a single CSV file
      resultDF.coalesce(1)
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv("RangeReportResult")

      println("Crime data filtering completed. Output saved to RangeReportResult.")
    }
    finally {
      spark.stop()
    }
  }
}
