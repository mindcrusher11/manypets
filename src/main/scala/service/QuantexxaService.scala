package org.manypets.cam
package service

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions._
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
      .alias("Number of Flights")

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
      .groupBy("passengerId")
      .count()
      .sort(col("count").desc)
      .limit(100)

    val frequentFlyers = flyersData
      .join(passengersCount, "passengerId")
      .sort(col("count").desc)

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
      .groupBy("passengerId")
      .agg(
        // concat is for concatenate two lists of strings from columns "from" and "to"
        concat(
          // collect list gathers all values from the given column into array
          collect_list(col("from")),
          collect_list(col("to"))
        ).name("countries")
      )

    val passengerLongestRuns = dataWithCountries.withColumn(
      "longest_run",
      size(
        array_remove(array_distinct(col("countries")),
                     col("countries").getItem(0)))
    )
    val passengerMaxCountries =
      passengerLongestRuns.select("passengerId", "longest_run")

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
    val flewTogetherPassegers = inputData
      .as("df1")
      .join(
        inputData.as("df2"),
        col("df1.passengerId") < col("df2.passengerId") && col("df1.flightId") === col(
          "df2.flightId") &&
          col("df1.date") === col("df2.date"),
        "inner"
      )
      .groupBy(col("df1.passengerId"), col("df2.passengerId"))
      .agg(count("*").as("flightsTogether"))
      .filter(col("flightsTogether") > atLeastNTimes)
      .sort(col("flightsTogether").desc)

    flewTogetherPassegers
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
    val flewTogetherPassengersByDate = inputData
      .as("df1")
      .join(
        inputData.as("df2"),
        col("df1.passengerId") < col("df2.passengerId") && col("df1.flightId") === col(
          "df2.flightId") &&
          col("df1.date") === col("df2.date"),
        "inner"
      )
      .groupBy(col("df1.passengerId"), col("df2.passengerId"))
      .agg(count("*").as("flightsTogether"),
           min(col("df1.date")).as("from"),
           max(col("df1.date")).as("to"))
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

    //reading flights data from csv file
    val flightsDf =
      ReadFiles.readCSVFile(Option(
        "/home/gaur/Downloads/Flight_Data_Assignment/Flight Data Assignment/flightData.csv"))

    // reading passengers info data from csv file
    val passengerInfo = ReadFiles.readCSVFile(Option(
      "/home/gaur/Downloads/Flight_Data_Assignment/Flight Data Assignment/passengers.csv"))

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
