package org.manypets.cam
package service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, countDistinct, desc, size}

/**
 *
 * Class for implementing tasks in manypets
 *
 * @author Gaurhari
 *
* */
object ManyPetsTasksService {

  def getDifferentPolicies(policy: DataFrame): DataFrame ={
    val distinctpolicies = policy.select(countDistinct("uuid"))
      //.collect()(0)(0)
    distinctpolicies
  }

  def getAvgPetsPerPolicy(policy: DataFrame): DataFrame ={
    val breedbypolicy = policy.withColumn("petsSize", size(col("data.insured_entities")))
      .groupBy("uuid").agg(avg("petsSize").as("totalpets"))
    breedbypolicy
  }

  def getMostPopularBreed(policy: DataFrame): DataFrame ={
    policy.createTempView("policy")
    val breedData = policy.sqlContext.sql("Select explode(data.insured_entities.breed) AS breed from policy")
    breedData.groupBy("breed").count().sort(desc("count"))
  }

  def claimedPolicyCount(claims:DataFrame): DataFrame ={
    val claimPolicies = claims.select(countDistinct("uuid_policy")) //.collect()(0)(0)
    claimPolicies
  }

  def AvgClaimValueByBreed(policy:DataFrame, claims:DataFrame): DataFrame ={
    policy.createOrReplaceTempView("policy")
    val breedData = policy.sqlContext.sql("Select uuid ,explode(data.insured_entities.breed) AS breed from policy")
    val avgClaimByBreed = breedData.join(claims,breedData("uuid") === claims("uuid_policy"), "inner")
      .groupBy("breed").avg("payout").as("payout")
    avgClaimByBreed
  }
}
