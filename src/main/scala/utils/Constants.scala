package org.manypets.cam
package utils

import config.DataConfig

object Constants {

  val config = DataConfig.getConfig()

  val sparkMasterUrl = config.getString("spark.masterurl")
  val sparkAppName = config.getString("spark.appName")
  val checkPointDir = config.getString("spark.checkpointDir")
  val batchDuration = config.getInt("spark.batchDuration")

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

}
