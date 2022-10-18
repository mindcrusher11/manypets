package org.manypets.cam
package service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{
  avg,
  broadcast,
  col,
  countDistinct,
  desc,
  size
}
import org.manypets.cam.utils.Constants

/**
  *
  * Class for implementing tasks in manypets
  *
  * @author Gaurhari
  *
  * */
object ManyPetsTasksService {

  /**
    * function to get different policies
    *
    * @param policy
    *
    * @return DataFrame
    * */
  def getDifferentPolicies(policy: DataFrame): DataFrame = {
    val distinctpolicies =
      policy.select(
        countDistinct(Constants.policyUuidColumn)
          .as(Constants.uniquePolicyCount))
    //.collect()(0)(0)
    distinctpolicies
  }

  /**
    * function to get avg number of pets per policy
    *
    * @param policy
    *
    * @return DataFrame
    * */
  def getAvgPetsPerPolicy(policy: DataFrame): DataFrame = {
    val breedbypolicy = policy
      .withColumn(Constants.policyPetsSize,
                  size(col(Constants.policyInsuredEntity)))
      .groupBy(Constants.policyUuidColumn)
      .agg(avg(Constants.policyPetsSize).as(Constants.totalPets))
    breedbypolicy
  }

  /**
    * function to get most popular breed
    *
    * @param policy
    *
    * @return DataFrame
    *
    * */
  def getMostPopularBreed(policy: DataFrame): DataFrame = {
    policy.createTempView(Constants.policyTempView)
    val breedData = policy.sqlContext.sql(
      "Select explode(data.insured_entities.breed) AS breed from policy")
    breedData.groupBy(Constants.policyBreed).count().sort(desc(Constants.count))
  }

  /**
    * function to get find number of policies claimed
    *
    * @param policy
    * @return DataFrame
    * */
  def claimedPolicyCount(claims: DataFrame): DataFrame = {
    val claimPolicies = claims.select(
      countDistinct(Constants.uuidPolicyColumn)
        .as(Constants.claimedPoliciesCount)) //.collect()(0)(0)
    claimPolicies
  }

  /**
    * function to find average claim value by breed
    *
    * @param policy
    *
    * @return DataFrame
    * */
  def AvgClaimValueByBreed(policy: DataFrame, claims: DataFrame): DataFrame = {

    policy.createOrReplaceTempView(Constants.policyTempView)
    val breedData = policy.sqlContext.sql(
      "Select uuid ,explode(data.insured_entities.breed) AS breed from policy")
    val avgClaimByBreed = breedData
      .join(broadcast(claims),
            breedData(Constants.policyUuidColumn) === claims(
              Constants.uuidPolicyColumn),
            Constants.innerJoin)
      .groupBy(Constants.policyBreed)
      .avg(Constants.claimPayout)
      .as(Constants.claimPayout)
    avgClaimByBreed
  }
}
