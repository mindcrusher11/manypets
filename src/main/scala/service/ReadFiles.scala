package org.manypets.cam
package service

import config.SparkConfig
import iservice.TReadFiles
import utils.Constants

import org.apache.spark.sql.DataFrame

/**
  * Class for implementation of reading files from different sources
  *
  * @author Gaurhari
  * */
object ReadFiles extends TReadFiles {

  /**
    * function to read csv files using spark
    *
    * @param filePath path of csv file
    *
    * @return DataFrame
    *
    * @throws Exception
    * */
  override def readCSVFile(filePath: Option[String]): DataFrame = {
    filePath match {
      case Some(x) => {
        val segments = SparkConfig.getSparkSession.read
          .option(Constants.fileSeparator, Constants.commaSeparator)
          .option(Constants.header, Constants.trueValue)
          .option(Constants.inferSchema, Constants.trueValue)
          .csv(x)
        segments
      }
      case None =>
        throw new Exception(
          Constants.invalidPathExceptionMessage
        )
    }

  }

  /**
    *
    * function to read json files in spark
    *
    * @author Gaurhari
    *
    * @param filePath path of json files
    *
    * @return DataFrame
    *
    * @throws Exception
    *
    * */
  override def readJsonFile(filePath: Option[String]): DataFrame = {
    filePath match {
      case Some(x) => {
        val segments = SparkConfig.getSparkSession.read
          .option(Constants.multiline, Constants.trueValue)
          .json(x)
        segments
      }
      case None =>
        throw new Exception(
          Constants.invalidPathExceptionMessage
        )
    }
  }
}
