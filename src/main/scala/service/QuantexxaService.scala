package org.manypets.cam
package service

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions._
import org.manypets.cam.config.DataConfig
import org.manypets.cam.utils.QuantexxaConstants
import java.sql.Date

/*
 *
 * Service to implement quantexxa tasks
 *
 * @author Gaurhari
 *
 * */
object QuantexxaService {

  /*
   * function to get monthly count of flights
   *
   * @param inputData input dataframe with flights information
   *
   * @return DataFrame
   * */
  def getMonthlyFlights(inputData: DataFrame): DataFrame = {

    val monthlyFlights = inputData
      .groupBy(month(col("date")).alias("month"))
      .count()
      .alias("numberOfFlights")

    monthlyFlights

  }

  /*
   * function to get top 100 frequent flyers
   *
   * @param inputData input data of flights
   * @param flyersData user data of flyers
   *
   * @return DataFrame frequentflyers dataframe
   *
   * */
  def getFrequentFlyers(inputData: DataFrame,
                        flyersData: DataFrame): DataFrame = {

    val passengersCount = inputData
      .groupBy(QuantexxaConstants.passengerIdColumn)
      .count()
      .sort(col(QuantexxaConstants.countColumn).desc)
      .limit(100)

    val frequentFlyers = flyersData
      .join(passengersCount, QuantexxaConstants.passengerIdColumn)
      .sort(col(QuantexxaConstants.countColumn).desc)

    frequentFlyers

  }

  /*
   * function to get maxcountries by passenger
   *
   * @param inputData Dataframe of flyersData
   *
   * @return DataFrame
   * */
  def getPassengerMaxCountries(inputData: DataFrame): DataFrame = {

    val dataWithCountries = inputData
      .groupBy(QuantexxaConstants.passengerIdColumn)
      .agg(
        // concat is for concatenate two lists of strings from columns "from" and "to"
        concat(
          // collect list gathers all values from the given column into array
          collect_list(col(QuantexxaConstants.fromDateColumn)),
          collect_list(col(QuantexxaConstants.toDateColumn))
        ).name(QuantexxaConstants.countriesColumn)
      )

    val passengerLongestRuns = dataWithCountries.withColumn(
      "longestRun",
      size(
        array_remove(array_distinct(col(QuantexxaConstants.countriesColumn)),
                     col(QuantexxaConstants.countriesColumn).getItem(0)))
    )

    val passengerMaxCountries =
      passengerLongestRuns.select(QuantexxaConstants.passengerIdColumn,
                                  QuantexxaConstants.longestRunColumn)

    passengerMaxCountries

  }

  /*
   *function to get passengers who flew together atleast N times
   *
   *@param inputData flights dataframe
   *@param atLeastNTimes minimum times flew together
   *
   *@return DataFrame
   *
   * */
  def getFlewTogetherPassengers(inputData: DataFrame,
                                atLeastNTimes: Int): DataFrame = {

    val flewTogetherPassegers =
      getCommonFlewTogetherDf(inputData, atLeastNTimes)
        .groupBy(col("flightDf.passengerId"), col("flightDf1.passengerId"))
        .agg(count("*").as("flightsTogether"))
        .filter(col("flightsTogether") > atLeastNTimes)
        .sort(col("flightsTogether").desc)

    flewTogetherPassegers
  }

  def getCommonFlewTogetherDf(inputData: DataFrame,
                              atLeastNTimes: Int): DataFrame = {
    val flewTogetherPassengers = inputData
      .as("flightDf")
      .join(
        inputData.as("flightDf1"),
        col("flightDf.passengerId") < col("flightDf1.passengerId") && col(
          "flightDf.flightId") === col("flightDf1.flightId") &&
          col("flightDf.date") === col("flightDf1.date"),
        "inner"
      )

    flewTogetherPassengers
  }

  /*
   *
   *function to get passengers who flew together atleast N times from one date to another date
   *
   *@param inputData flights dataframe
   *@param atLeastNTimes minimum times flew together
   *@param from  from date
   *@param to  to date
   *
   *@return DataFrame
   *
   * */
  def getFlewTogetherPassengersByDate(inputData: DataFrame,
                                      atLeastNTimes: Int,
                                      from: Date = null,
                                      to: Date = null): DataFrame = {

    val flewTogetherPassengersByDate =
      getCommonFlewTogetherDf(inputData, atLeastNTimes)
        .groupBy(col("flightDf.passengerId"), col("flightDf1.passengerId"))
        .agg(count("*").as("flightsTogether"),
             min(col("flightDf.date")).as("from"),
             max(col("flightDf.date")).as("to"))
        .filter(col("from") > from && col("to") < to)
        .sort(col("flightsTogether").desc)

    flewTogetherPassengersByDate
  }

  /*
   *
   * main function to call all functions
   *
   * @param args Array of String
   *
   * */
  def main(args: Array[String]): Unit = {

    // reading flights info data from csv file
    val flightsDf =
      ReadFiles.readCSVFile(
        Option(DataConfig.getConfig().getString("file.flightsDataPath")))

    // reading passengers info data from csv file
    val passengerInfo = ReadFiles.readCSVFile(
      Option(DataConfig.getConfig().getString("file.passengersDataPath")))

    getMonthlyFlights(flightsDf).show
    getFrequentFlyers(flightsDf, passengerInfo).show
    getPassengerMaxCountries(flightsDf).show
    getFlewTogetherPassengers(flightsDf, 5).show
    getFlewTogetherPassengersByDate(flightsDf,
                                    5,
                                    Date.valueOf("2017-01-10"),
                                    Date.valueOf("2017-05-12")).show
  }

}
