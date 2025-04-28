package org.manypets.cam
package iservice

import org.apache.spark.sql.DataFrame

/**
 * Interface definition for defining asbtract functions for Sahaj Service tasks
 *
 * @author Gaurhari
 *
 * */
trait TSahajService {

  def cleanData(df: DataFrame): DataFrame

  def calculateAvgPriceByNeighbourhood(df: DataFrame): DataFrame

  def calculatePriceComparison(df: DataFrame): DataFrame

  def saveResults(avgPrice: DataFrame, priceComparison: DataFrame): Unit

  def process(): Unit
}
