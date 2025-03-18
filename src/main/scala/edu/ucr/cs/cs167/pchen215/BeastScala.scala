package edu.ucr.cs.cs167.pchen215

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Scala program for Temporal Crime Analysis using Beast and Spark
 */
object BeastScala {
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
    val startDate = args(0)  // Expected format: MM/DD/YYYY
    val endDate = args(1)    // Expected format: MM/DD/YYYY

    try {
      // Load crime dataset from Parquet format
      val crimeDF = spark.read.parquet("Chicago_Crimes_ZIP.parquet")

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


//import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
//import org.apache.spark.SparkConf
//import org.apache.spark.beast.SparkSQLRegistration
//import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//import org.apache.spark.sql.functions._
//
///**
// * Scala program for Temporal Crime Analysis using Beast and Spark
// */
//object BeastScala {
//  def main(args: Array[String]): Unit = {
//    // Initialize Spark context
//    val conf = new SparkConf().setAppName("Beast Example")
//
//    // Set Spark master to local if not already set
//    if (!conf.contains("spark.master"))
//      conf.setMaster("local[*]")
//
//    val spark = SparkSession.builder().config(conf).getOrCreate()
//    val sparkContext = spark.sparkContext
//
//    // Register Beast functions
//    SparkSQLRegistration.registerUDT
//    SparkSQLRegistration.registerUDF(spark)
//
//    // Import necessary functions
//    import spark.implicits._
//    import edu.ucr.cs.bdlab.beast._
//
//    // Check if correct arguments are provided
//    if (args.length < 2) {
//      println("Usage: spark-submit CrimeTemporalAnalysis.scala <start_date> <end_date>")
//      sys.exit(1)
//    }
//
//    // Parse command-line arguments
//    val startDate = args(0)  // Expected format: MM/DD/YYYY
//    val endDate = args(1)    // Expected format: MM/DD/YYYY
//
//    try {
//      // Load crime dataset from Parquet format
//      val crimeDF = spark.read.parquet("Chicago_Crimes_ZIP.parquet")
//
//      // Convert Date column to Timestamp format
//      val crimeWithDateDF = crimeDF.withColumn("ParsedDate", to_timestamp($"Date", "MM/dd/yyyy hh:mm:ss a"))
//
//      // Convert start and end dates to Date format
//      val parsedStartDate = to_date(lit(startDate), "MM/dd/yyyy")
//      val parsedEndDate = to_date(lit(endDate), "MM/dd/yyyy")
//
//      // Filter crimes that occurred within the given date range
//      val filteredDF = crimeWithDateDF
//        .filter($"ParsedDate".between(parsedStartDate, parsedEndDate))
//
//      // Group by crime type and count occurrences
//      val crimeCountsDF = filteredDF.groupBy("PrimaryType").count()
//
//      // Save results as a CSV file
//      crimeCountsDF.coalesce(1)
//        .write
//        .option("header", "true")
//        .mode(SaveMode.Overwrite)
//        .csv("CrimeTypeCount")
//
//      println("CrimeTypeCount.csv has been generated successfully!")
//
//    } catch {
//      case e: Exception =>
//        println(s"Error: ${e.getMessage}")
//        e.printStackTrace()
//    } finally {
//      spark.stop()
//    }
//  }
//}

//import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
//import org.apache.spark.SparkConf
//import org.apache.spark.beast.SparkSQLRegistration
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//
//import scala.collection.Map
//
///**
// * Scala examples for Beast
// */
//object BeastScala {
//  def main(args: Array[String]): Unit = {
//    // Initialize Spark context
//
//    val conf = new SparkConf().setAppName("Beast Example")
//    // Set Spark master to local if not already set
//    if (!conf.contains("spark.master"))
//      conf.setMaster("local[*]")
//
//    val spark: SparkSession.Builder = SparkSession.builder().config(conf)
//
//    val sparkSession: SparkSession = spark.getOrCreate()
//    val sparkContext = sparkSession.sparkContext
//    SparkSQLRegistration.registerUDT
//    SparkSQLRegistration.registerUDF(sparkSession)
//
//    // Check if correct arguments are provided
//    if (args.length < 2) {
//      println("Usage: spark-submit CrimeTemporalAnalysis.scala <start_date> <end_date>")
//      sys.exit(1)
//    }
//
//    // Parse command-line arguments
//    val startDate = args(0)  // Expected format: MM/DD/YYYY
//    val endDate = args(1)    // Expected format: MM/DD/YYYY
//
//    try {
//      // Import Beast features
//      import edu.ucr.cs.bdlab.beast._
//
//      // Load crime dataset from Parquet format
//      val crimeDF = spark.read.parquet("Chicago_Crimes_ZIP.parquet")
//
//      // Convert Date column to Timestamp format
//      val crimeWithDateDF = crimeDF.withColumn("ParsedDate", to_timestamp($"Date", "MM/dd/yyyy hh:mm:ss a"))
//
//      // Convert start and end dates to Date format
//      val parsedStartDate = spark.sql(s"SELECT to_date('$startDate', 'MM/dd/yyyy')").collect()(0)(0)
//      val parsedEndDate = spark.sql(s"SELECT to_date('$endDate', 'MM/dd/yyyy')").collect()(0)(0)
//
//      // Filter crimes that occurred within the given date range
//      val filteredDF = crimeWithDateDF
//        .filter($"ParsedDate".between(parsedStartDate, parsedEndDate))
//
//      // Group by crime type and count occurrences
//      val crimeCountsDF = filteredDF.groupBy("PrimaryType").count()
//
//      // Save results as a CSV file
//      crimeCountsDF.coalesce(1)
//        .write
//        .option("header", "true")
//        .csv("CrimeTypeCount")
//
//      println("CrimeTypeCount.csv has been generated successfully!")
//
//    } finally {
//      sparkSession.stop()
//    }
//  }
//}

//case "count-by-keyword" =>
//  val keyword: String = args(2)
//  // TODO count the number of occurrences of each keyword per county and display on the screen
//  sparkSession.read.parquet(inputFile)
//    .createOrReplaceTempView("tweets")
//  println("CountyID\tCount")
//  sparkSession.sql(
//    s"""
//      SELECT CountyID, count(*) AS count
//      FROM tweets
//      WHERE array_contains(keywords, "$keyword")
//      GROUP BY CountyID
//    """).foreach(row => println(s"${row.get(0)}\t${row.get(1)}"))
//case "choropleth-map" =>
//  val keyword: String = args(2)
//  val outputFile: String = args(3)
//  // TODO write a Shapefile that contains the count of the given keyword by county
//  sparkSession.read.parquet(inputFile)
//    .createOrReplaceTempView("tweets")
//  val keywordCountsDF = sparkSession.sql(
//    s"""
//      SELECT CountyID, count(*) AS count
//      FROM tweets
//      WHERE array_contains(keywords, "$keyword")
//      GROUP BY CountyID
//    """)
//  keywordCountsDF.createOrReplaceTempView("keyword_counts")
//  sparkSession.read.format("shapefile").load("tl_2018_us_county.zip")
//    .createOrReplaceTempView("counties")
//  val joinedDF = sparkSession.sql(
//    s"""
//      SELECT keyword_counts.CountyID, counties.NAME, counties.geometry, keyword_counts.count
//      FROM keyword_counts
//      JOIN counties ON keyword_counts.CountyID = counties.GEOID
//    """)
//  val choroplethRDD = joinedDF.toSpatialRDD
//  choroplethRDD.coalesce(1).saveAsShapefile(outputFile)