package edu.ucr.cs.cs167.master

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object TaskA1 {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: DataPreparation <input_csv_path> <output_parquet_path>")
      sys.exit(1)
    }

    val inputCsvPath = args(0)
    val outputParquetPath = args(1)

    // Initialize Spark context with increased memory
    val conf = new SparkConf()
      .setAppName("Crime Data Preparation")
      .setMaster("local[*]")
      // Add memory configuration
      .set("spark.driver.memory", "4g")
      .set("spark.executor.memory", "4g")
      // Tune garbage collection for better memory management
      .set("spark.memory.fraction", "0.8")
      // Increase parallelism
      .set("spark.default.parallelism", "8")
      // Ensure shuffle service works efficiently
      .set("spark.shuffle.service.enabled", "true")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)
    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    try {
      // Load Chicago Crime CSV file with more efficient reading
      println("Loading CSV data...")
      val crimeDF = sparkSession.read
        .option("header", "true")
        .option("delimiter", ",")
        .option("mode", "DROPMALFORMED") // Skip malformed records
        .csv(inputCsvPath)

      // Rename columns with spaces - no changes needed here
      println("Renaming columns...")
      val renamedDF = crimeDF
        .withColumnRenamed("X Coordinate", "XCoordinate")
        .withColumnRenamed("Y Coordinate", "YCoordinate")
        .withColumnRenamed("Case Number", "CaseNumber")
        .withColumnRenamed("Primary Type", "PrimaryType")
        .withColumnRenamed("Location Description", "LocationDescription")
        .withColumnRenamed("Community Area", "CommunityArea")
        .withColumnRenamed("FBI Code", "FBICode")
        .withColumnRenamed("Updated On", "UpdatedOn")

      // Create a temporary view to use SQL
      renamedDF.createOrReplaceTempView("crimes_temp")

      // Process data with correct column order and types
      println("Processing data with SQL...")
      val processedDF = sparkSession.sql("""
        SELECT
          CAST(x AS DOUBLE) AS x,
          CAST(y AS DOUBLE) AS y,
          CAST(ID AS INT) AS ID,
          CaseNumber,
          Date,
          Block,
          IUCR,
          PrimaryType,
          Description,
          LocationDescription,
          Arrest,
          Domestic,
          Beat,
          District,
          Ward,
          CAST(CommunityArea AS INT) AS CommunityArea,
          FBICode,
          CAST(XCoordinate AS INT) AS XCoordinate,
          YCoordinate,
          Year,
          UpdatedOn
        FROM crimes_temp
        WHERE x IS NOT NULL AND y IS NOT NULL
      """)

      // Create a geometry point
      println("Creating geometry points...")
      processedDF.createOrReplaceTempView("crimes_processed")
      val crimesWithGeometryDF = sparkSession.sql("""
        SELECT *, ST_CreatePoint(x, y) AS geometry
        FROM crimes_processed
      """)

      // Convert to SpatialRDD with repartitioning for better memory distribution
      println("Converting to SpatialRDD...")
      val numPartitions = 8 // Adjust based on your cluster size
      val crimeRDD = crimesWithGeometryDF.repartition(numPartitions).toSpatialRDD

      // Persist the RDD to avoid recomputation
      crimeRDD.persist(StorageLevel.MEMORY_AND_DISK)

      // Load the ZIP Code dataset
      println("Loading ZIP Code boundaries...")
      val zipCodesRDD = sparkContext.shapefile("tl_2018_us_zcta510.zip")

      // If the ZIP code dataset is small enough, consider broadcasting
      // For now, we'll do a regular spatial join but with better partitioning
      zipCodesRDD.persist(StorageLevel.MEMORY_AND_DISK)

      // Perform spatial join with a tracking message
      println("Performing spatial join...")
      val crimeZipJoin = crimeRDD.spatialJoin(zipCodesRDD)

      // Process in batches with progress tracking
      println("Processing join results...")
      val crimeWithZipRDD = crimeZipJoin.map { case (crime, zipCode) =>
        Feature.append(crime.asInstanceOf[IFeature], zipCode.getAs[String]("ZCTA5CE10"), "ZIPCode")
      }

      // We can unpersist the original RDDs now
      crimeRDD.unpersist()
      zipCodesRDD.unpersist()

      // Convert back to DataFrame and drop geometry
      println("Converting to DataFrame...")
      val crimeWithZipDF = crimeWithZipRDD.toDataFrame(sparkSession)
      val finalDF = crimeWithZipDF.drop("geometry")

      // Write to Parquet with optimized settings
      println("Writing to Parquet...")
      finalDF.write
        .mode(SaveMode.Overwrite)
        .option("compression", "snappy") // Use snappy compression for better performance
        .parquet(outputParquetPath)

      // Read back and verify schema
      println("Verifying schema...")
      val parquetDF = sparkSession.read.parquet(outputParquetPath)
      println("Schema of Parquet file:")
      parquetDF.printSchema()

      // Calculate file sizes only for small datasets (don't do for 100k)
      if (inputCsvPath.contains("1k") || inputCsvPath.contains("10k")) {
        val csvSize = new java.io.File(inputCsvPath).length()
        val parquetSize = calculateDirectorySize(new java.io.File(outputParquetPath))
        println(s"CSV size: $csvSize bytes")
        println(s"Parquet size: $parquetSize bytes")
      } else {
        println("Skipping file size calculation for large datasets")
      }

      println("Processing completed successfully!")

    } catch {
      case e: Exception =>
        println(s"Error processing data: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      sparkSession.stop()
    }
  }

  def calculateDirectorySize(dir: java.io.File): Long = {
    if (dir.isFile) return dir.length()
    var size: Long = 0
    val files = dir.listFiles()
    if (files != null) {
      for (file <- files) {
        if (file.isFile) {
          size += file.length()
        } else {
          size += calculateDirectorySize(file)
        }
      }
    }
    size
  }
}

