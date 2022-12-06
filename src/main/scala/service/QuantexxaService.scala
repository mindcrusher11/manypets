package org.manypets.cam
package service

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions._
import java.sql.Date

object QuantexxaService {

  def getMonthlyFlights(inputData: DataFrame): DataFrame = {
    val monthlyFlights = inputData
      .groupBy(month(col("date")).alias("month"))
      .count()
      .alias("Number of Flights")

    monthlyFlights
  }

  def getFrequentFlyers(inputData: DataFrame,
                        flyersData: DataFrame): DataFrame = {
    val passengersCount = inputData
      .groupBy("passengerId")
      .count()
      .sort(col("count").desc)
      .limit(100)

    println("passengers count")
    passengersCount.show()

    val frequentFlyers = flyersData
      .join(passengersCount, "passengerId")
      .sort(col("count").desc)

    frequentFlyers
  }

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

  def getFlewTogetherPassengers(inputData: DataFrame,
                                atLeastNTimes: Int,
                                from: Date = null,
                                to: Date = null): DataFrame = {
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
  def main(args: Array[String]): Unit = {
    val flightsDf =
      ReadFiles.readCSVFile(Option(
        "/home/gaur/Downloads/Flight_Data_Assignment/Flight Data Assignment/flightData.csv"))
    val passengerInfo = ReadFiles.readCSVFile(Option(
      "/home/gaur/Downloads/Flight_Data_Assignment/Flight Data Assignment/passengers.csv"))
    getMonthlyFlights(flightsDf).show
    getFrequentFlyers(flightsDf, passengerInfo)
    passengerInfo.show()
    getFlewTogetherPassengers(flightsDf, 5)
    getFlewTogetherPassengersByDate(flightsDf,
                                    5,
                                    Date.valueOf("2017-01-10"),
                                    Date.valueOf("2017-05-12"))
    getPassengerMaxCountries(flightsDf)
  }
}
