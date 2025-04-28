package org.manypets.cam
package utils

import config.DataConfig

import com.typesafe.config.Config
import org.manypets.cam.iservice.TSahajService

object Constants {

  val config: Config = DataConfig.getConfig()

  val sparkMasterUrl: String = config.getString("spark.masterurl")
  val sparkAppName: String = config.getString("spark.appName")
  val checkPointDir: String = config.getString("spark.checkpointDir")
  val batchDuration: Int = config.getInt("spark.batchDuration")

  val error = "ERROR"

  val appConfFile = "application.conf"

  val slash = "/"

  val multiline = "multiline"

  val trueValue = "true"

  /*
   * file options for spark
   * */
  val fileSeparator = "sep"
  val commaSeparator = ","
  val tabSeparator = "\t"
  val header = "header"
  val inferSchema = "inferSchema"
  val invalidPathExceptionMessage = "Please input valid path"

  /*
   * manypets files columns
   * */

  val policyUuidColumn = "uuid"
  val policyPetsSize = "petsSize"
  val totalPets = "totalpets"
  val policyInsuredEntity = "data.insured_entities"
  val policyTempView = "policy"
  val policyBreed = "breed"
  val count = "count"
  val uuidPolicyColumn = "uuid_policy"
  val claimPayout = "payout"
  val innerJoin = "inner"
  val claimedPoliciesCount = "claimedPoliciesCount"
  val uniquePolicyCount = "policyCount"


  /*
  * Sahaj Service Files Columns
  * */
  val priceColumn = "price"
  val lastScrapedColumn = "last_scraped"
  val lastScrapedMonthCol = "last_scraped_month"
  val neighbourhoodCleansedCol = "neighbourhood_cleansed"
  val idCol = "id"
  val nameCol = "name"
  val priceNumericCol = "price_numeric"
  val neighborhoodAvgPriceCol = "neighborhood_avg_price"
  val priceDifferenceCol = "price_difference"
  val priceStatusCol = "price_status"
  val avgPricePerNightCol = "avg_price_per_night"
  val listingCountCol = "listing_count"
  val overpriced = "Overpriced"
  val underpriced = "Underpriced"
  val average = "Average"



}

