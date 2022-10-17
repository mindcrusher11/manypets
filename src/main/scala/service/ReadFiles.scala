package org.manypets.cam
package service

import iservice.TReadFiles

import org.apache.spark.sql.DataFrame
import org.manypets.cam.config.SparkConfig

/**
 * Class for implementation of reading files from different sources
 *
 * @author Gaurhari
 * */
object ReadFiles extends TReadFiles{

  override def readCSVFile(filePath: Option[String]): DataFrame = {
    filePath match {
      case Some(x) => {
        val segments = SparkConfig.getSparkSession.read
        .option("sep", ",")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(x)
        segments
      }
      case None => throw new Exception(
        "Please input valid path"
      )
    }

  }

  override def readJsonFile(filePath: Option[String]): DataFrame = {

    filePath match {
      case Some(x) => {
        val segments = SparkConfig.getSparkSession.read.option("multiline","true")
          .json(x)
        segments
      }
      case None => throw new Exception(
        "Please input valid path"
      )
    }
  }
}
