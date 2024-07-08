package org.manypets.cam
package service

import config.DataConfig

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, desc, min}
import org.manypets.cam.utils.{Constants, NBCConstants}

object NBCUniversal {

  /**
   * function to read f1Racing csv file
   *
   * @param filePath String Path
   *
   * @return DataFrame
   *
   * */
  def getF1DataFrame(filePath:String ): DataFrame = {
    val f1DataDf =
      ReadFiles.readCSVFile(
        Option(DataConfig.getConfig().getString("file.f1racingDataPath")))

    f1DataDf
  }

  /**
   *
   * Function to return Avg and Fast time of each driver
   *
   * @param f1DataDf DataFrame
   *
   * @return DataFrame
   * */
  def getDriverAvgAndFastTime(f1DataDf: DataFrame): DataFrame = {
    f1DataDf.groupBy(NBCConstants.driver)
      .agg(avg(NBCConstants.drivingTime).as(NBCConstants.avgTime), min(NBCConstants.drivingTime)
        .as(NBCConstants.fasttime)).sort(NBCConstants.avgTime)
  }


  /**
   *
   * function to read top N number of drivers
   *
   * @param f1DataDf DataFrame
   *
   * @param topN Int
   *
   * @return DataFrame
   * */
  def getTopDrivers(f1DataDf: DataFrame, topN: Int): DataFrame = {
    f1DataDf.limit(topN)
  }


  /*
  * Main function to execute the tasks
  * */
  def main(args: Array[String]): Unit = {

    // reading flights info data from csv file and get

    val driveravgandlowest = getDriverAvgAndFastTime(getF1DataFrame(""))

    getTopDrivers(driveravgandlowest, 3).show


  }

}
