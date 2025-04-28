package org.manypets.cam
package service

import config.DataConfig

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, date_format, regexp_replace, to_date, when}
import iservice.TSahajService
import utils.Constants

/**
 *
 * Class for implementing tasks in Sahaj task
 *
 * @author Gaurhari
 *
 * */
object SahajSolutionService extends TSahajService{

  /*
  *
  * Process all the logic here for sequence of steps
  *
  * */
  def process(): Unit = {
    val listingsDF =
      ReadFiles.readCSVFile(
        Option(DataConfig.getConfig().getString("file.sahajDataFilePath")))

    listingsDF.printSchema()

    //flightsDf.select("host_neighbourhood").show()

    // Clean and transform raw data for analysis
    val cleanedDF = cleanData(listingsDF)

    // Calculate average price per night by neighborhood and month
    val avgPriceByNeighborhood = calculateAvgPriceByNeighbourhood(cleanedDF)

    // Identify top 10 overpriced and underpriced listings compared to neighborhood averages
    val priceComparison = calculatePriceComparison(cleanedDF)

    // Save results to parquet files for efficient storage and querying
    saveResults(avgPriceByNeighborhood, priceComparison)

    listingsDF.select(Constants.idCol,Constants.neighbourhoodCleansedCol, Constants.lastScrapedColumn,Constants.priceColumn)
    cleanedDF.select(Constants.idCol,Constants.neighbourhoodCleansedCol, Constants.lastScrapedMonthCol,Constants.priceNumericCol).show
    avgPriceByNeighborhood.show(10, truncate = false)
    priceComparison.show()
  }

  /**
   * Cleans and transforms raw data by extracting relevant columns and converting price to numeric format.
   *
   *
   * @param df Input DataFrame containing raw listing data
   *
   * @return Cleaned DataFrame with selected columns and transformed price
   */
  def cleanData(df: DataFrame): DataFrame = {
    df.withColumn(Constants.priceNumericCol,
        regexp_replace(col(Constants.priceColumn), "[$,]", "").cast("double"))
      .withColumn(Constants.lastScrapedMonthCol,
        date_format(to_date(col(Constants.lastScrapedColumn)), "yyyy-MM"))
      .filter(col(Constants.priceNumericCol).isNotNull && col(Constants.neighbourhoodCleansedCol).isNotNull)
      .select(
        Constants.idCol,
        Constants.nameCol,
        Constants.neighbourhoodCleansedCol,
        Constants.priceNumericCol,
        Constants.lastScrapedMonthCol
      )
  }

  /**
   * Calculates the average price per night by neighborhood and month.
   *
   *
   * @param df Cleaned DataFrame with listing data
   *
   * @return DataFrame with average price and listing count per neighborhood and month
   *
   */
  def calculateAvgPriceByNeighbourhood(df: DataFrame): DataFrame = {
    df.groupBy(Constants.neighbourhoodCleansedCol, Constants.lastScrapedMonthCol)
      .agg(
        avg(Constants.priceNumericCol).alias(Constants.avgPricePerNightCol),
        count("*").alias(Constants.listingCountCol)
      )
      .orderBy(Constants.neighbourhoodCleansedCol, Constants.lastScrapedMonthCol)
  }

  /**
   * Identifies top 10 overpriced and underpriced listings by comparing listing price to neighborhood average.
   *
   *
   * @param df Cleaned DataFrame with listing data
   *
   * @return DataFrame with top 10 overpriced and underpriced listings
   */
  def calculatePriceComparison(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy(Constants.neighbourhoodCleansedCol, Constants.lastScrapedMonthCol)

    //Average price for neighbhourhood and month
    val withAvgPrice = df.withColumn(Constants.neighborhoodAvgPriceCol,
      avg(Constants.priceNumericCol).over(windowSpec))

    // Get price difference based on neighbourhood_avg_price
    val priceDiff = withAvgPrice.withColumn(Constants.priceDifferenceCol,
        col(Constants.priceNumericCol) - col(Constants.neighborhoodAvgPriceCol))
      .withColumn(Constants.priceStatusCol,
        when(col(Constants.priceDifferenceCol) > 0, Constants.overpriced)
          .when(col(Constants.priceDifferenceCol) < 0, Constants.underpriced)
          .otherwise(Constants.average))

    // Get top 10 overpriced night price
    val overpriced = priceDiff.filter(col(Constants.priceStatusCol) === Constants.overpriced)
      .orderBy(col(Constants.priceDifferenceCol).desc)
      .limit(10)

    // Get top 10 underpriced night price
    val underpriced = priceDiff.filter(col(Constants.priceStatusCol) === Constants.underpriced)
      .orderBy(col(Constants.priceDifferenceCol))
      .limit(10)

    // union of underpriced and overpriced data
    overpriced.union(underpriced)
      .select(
        Constants.idCol,
        Constants.nameCol,
        Constants.neighbourhoodCleansedCol,
        Constants.priceNumericCol,
        Constants.neighborhoodAvgPriceCol,
        Constants.priceDifferenceCol,
        Constants.priceStatusCol,
        Constants.lastScrapedMonthCol
      )
  }

  /**
   * Saves analysis results to parquet files, partitioned by month for efficient querying.
   *
   *
   * @param avgPrice DataFrame with average price by neighborhood
   *
   * @param priceComparison DataFrame with overpriced/underpriced listings
   *
   */
  def saveResults(avgPrice: DataFrame, priceComparison: DataFrame): Unit = {
    // Save to parquet for efficient storage and querying
    avgPrice.write
      .mode("overwrite")
      .partitionBy(Constants.lastScrapedMonthCol)
      .parquet("output/avg_price_by_neighborhood")

    priceComparison.write
      .mode("overwrite")
      .partitionBy(Constants.lastScrapedMonthCol)
      .parquet("output/price_comparison")
  }

  /*
  * Main function for calling method
  * */
  def main(args:Array[String]) : Unit = {
    process()
  }

}
