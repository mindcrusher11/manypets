package org.manypets.cam

import config.{DataConfig, SparkConfig}
import service.{ManyPetsTasksService, ReadFiles}
import utils.Constants

object Main {
  def main(args: Array[String]): Unit = {

    SparkConfig.getSparkSession.sparkContext.setLogLevel(Constants.error)

    val claimsData = ReadFiles.readCSVFile(
      Option(DataConfig.getConfig().getString("file.claimsPath")))
    val policyData = ReadFiles.readJsonFile(
      Option(DataConfig.getConfig().getString("file.policiesPath")))

    ManyPetsTasksService.getDifferentPolicies(policyData).show()

    ManyPetsTasksService.getAvgPetsPerPolicy(policyData).show()

    ManyPetsTasksService.getMostPopularBreed(policyData).show()

    ManyPetsTasksService.claimedPolicyCount(claimsData).show()

    ManyPetsTasksService
      .AvgClaimValueByBreed(policyData, claimsData)
      .show()
  }
}
