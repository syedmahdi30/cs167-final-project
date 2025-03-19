package edu.ucr.cs.cs167.master

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.Map

/**
 * Scala examples for Beast
 */
object TaskA2 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    //    val operation: String = args(0)
    val inputFile: String = args(0)
    val outputFile: String = args(1)

    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()

      // TODO: Run a grouped-aggregate SQL query that computes the total number of crimes per ZIP code.
      sparkSession.read.parquet(inputFile)
        .createOrReplaceTempView("crimes")
      //      println("ZIP Code\tCrimes")
      sparkSession.sql(
        s"""
            SELECT ZIPCode, COUNT(*) AS TotalCrimes
            FROM crimes
            GROUP BY ZIPCode
      """
      ).createOrReplaceTempView("crime_counts")
      //      .foreach(row => println(s"${row.get(0)}\t${row.get(1)}"))

      val zctaDF = sparkSession.read.format("shapefile").load("tl_2018_us_zcta510.zip")
        .createOrReplaceTempView("zipcodes")

      val result = sparkSession.sql(
        """
          SELECT z.ZCTA5CE10 AS ZIPCode, z.geometry, c.TotalCrimes
          FROM crime_counts c, zipcodes z
          WHERE c.ZIPCode = z.ZCTA5CE10
        """
      )

      val resultRDD = result.toSpatialRDD
      resultRDD.coalesce(1).saveAsShapefile(outputFile)

      val t2 = System.nanoTime()
      println(s"Operation on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
    } finally {
      sparkSession.stop()
    }
  }
}
