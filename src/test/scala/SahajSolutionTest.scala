package org.manypets.cam

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.manypets.cam.config.SparkConfig
import org.manypets.cam.service.SahajSolutionService
import org.scalatest.{FunSuite, Matchers}

class SahajSolutionTest extends FunSuite with Matchers with SharedSparkContext {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Set up logging
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
  }

  test("loadData should clean prices and filter invalid data") {
    val spark = SparkConfig.getSparkSession
    import spark.implicits._

    // Create test data
    val testData = Seq(
      ("1","test1", "$100.00", "Manhattan","2025-01-31"),
      ("2","test2", "$200,50", "Brooklyn","2025-01-31"),
      ("3","test3", "invalid", "Queens","2025-01-31"),
      ("4","test4", null, "Bronx","2025-01-31"),
      ("5","test5", "$150.00", null, "2025-01-31")
    )
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("price", StringType, nullable = true),
      StructField("neighbourhood_cleansed", StringType, nullable = true),
      StructField("last_scraped", StringType, nullable = true)
    ))
    val df = spark.createDataFrame(
      sc.parallelize(testData.map(Row.fromTuple)),
      schema
    )

    // Save test data to temporary CSV
    val tempDir = java.nio.file.Files.createTempDirectory("spark_test").toString
    df.write.mode("overwrite").csv(s"$tempDir/input")

    // Run loadData
    val resultDf = SahajSolutionService.cleanData(df)

    // Verify results
    val result = resultDf.collect().map(row => (
      row.getString(0),
      row.getDouble(row.fieldIndex("price_numeric")),
      row.getString(row.fieldIndex("neighbourhood_cleansed"))
    ))
    result should contain allOf (
      ("1", 100.0, "Manhattan"),
      ("2", 20050.0, "Brooklyn")
    )
    result.length shouldBe 2
  }

  test("computeAvgPriceByNeighborhood should calculate correct averages") {
    val spark = SparkConfig.getSparkSession
    import spark.implicits._

    // Create test data
    val testData = Seq(
      ("1", 100.0, "Manhattan","2025-01"),
      ("2", 200.0, "Manhattan","2025-01"),
      ("3", 150.0, "Brooklyn","2025-01")
    )
    val df = testData.toDF("id", "price_numeric", "neighbourhood_cleansed","last_scraped_month")

    // Run computeAvgPriceByNeighborhood
    val resultDf = SahajSolutionService.calculateAvgPriceByNeighbourhood(df)

    // Verify results
    val result = resultDf.collect().map(row => (
      row.getString(0),
      row.getString(1),
      row.getDouble(2)
    ))
    result should contain allOf (
      ("Manhattan", "2025-01", 150.0),
      ("Brooklyn", "2025-01", 150.0)
    )
  }

  test("computePricedListings should identify top over/under priced listings") {
    val spark = SparkConfig.getSparkSession
    import spark.implicits._

    // Create test data
    val testData = Seq(
      ("1","test1","url1", "Manhattan", 200.0,"2025-01"),
      ("2","test1","url2", "Manhattan", 100.0,"2025-01"),
      ("3", "test1", "url3", "Brooklyn", 300.0,"2025-01"),
      ("4", "test1" , "url4", "Brooklyn", 100.0,"2025-01")
    )
    val df = testData.toDF("id", "name","listing_url", "neighbourhood_cleansed", "price_numeric", "last_scraped_month")

    // Run computePricedListings
    val resultDf = SahajSolutionService.calculatePriceComparison(df)

    // Verify results
    val result = resultDf.collect().map(row => (
      row.getString(0),
      row.getDouble(3),
      row.getDouble(5)
    ))
    result.length shouldBe 4 // Top 2 overpriced + top 2 underpriced
    result should contain allOf (
      ("1", 200.0, 50.0),  // Overpriced in Manhattan (avg 150)
      ("2", 100.0, -50.0), // Underpriced in Manhattan
      ("3", 300.0, 100.0), // Overpriced in Brooklyn (avg 200)
      ("4", 100.0, -100.0) // Underpriced in Brooklyn
    )
  }
}