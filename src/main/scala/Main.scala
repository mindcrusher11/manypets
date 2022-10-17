package org.manypets.cam

import service.{ManyPetsTasksService, ReadFiles}

import breeze.linalg.sum
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{
  avg,
  col,
  count,
  countDistinct,
  desc,
  explode,
  length,
  size
}
import org.manypets.cam.config.SparkConfig
import org.manypets.cam.models.PolicyClaim
import org.manypets.cam.utils.HelpersFunctions
import org.manypets.cam.utils.HelpersFunctions.{flattenDataframe, recurs}

object Main {
  def main(args: Array[String]): Unit = {

    SparkConfig.getSparkSession.sparkContext.setLogLevel("ERROR")

    val claimsData = ReadFiles.readCSVFile(
      Option("/partition/DE_test_data_sets/DE_test_claims.csv"))
    val policyData = ReadFiles.readJsonFile(
      Option("/partition/DE_test_data_sets/lesspolicies"))

    ManyPetsTasksService.getDifferentPolicies(policyData).show()

    ManyPetsTasksService.getAvgPetsPerPolicy(policyData).show()

    ManyPetsTasksService.getMostPopularBreed(policyData).show()

    ManyPetsTasksService.claimedPolicyCount(claimsData).show()

    ManyPetsTasksService
      .AvgClaimValueByBreed(policyData, claimsData)
      .show()
  }
}
