package org.manypets.cam

import service.{ManyPetsTasksService, ReadFiles}

import breeze.linalg.sum
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, desc, explode, length, size}
import org.manypets.cam.config.SparkConfig
import org.manypets.cam.models.PolicyClaim
import org.manypets.cam.utils.HelpersFunctions
import org.manypets.cam.utils.HelpersFunctions.{flattenDataframe, recurs}

object Main {
  def main(args: Array[String]): Unit = {
    val claimsData = ReadFiles.readCSVFile(Option("/partition/DE_test_data_sets/DE_test_claims.csv"))
    val policyData = ReadFiles.readJsonFile(Option("/partition/DE_test_data_sets/lesspolicies"))
    //claimsData.show
    //claimsData.printSchema()
    //uuid_policy|date_of_loss|policy_reference|claim_outcome|payout


    /*
    number of policies in the dataset
    println(policyData.count())
    //
    val distinctpolicies = policyData.select(countDistinct("uuid")).collect()(0)(0)
    println(policyData.count())
    println(distinctpolicies)*/

    //policies claimed count
    /*val uniquepolicies = claimsData.select(countDistinct("uuid_policy")).collect()(0)(0)

    println(claimsData.count())
    println(uniquepolicies)*/


    //explode breeds data
    /*    val explodateData = policyData.withColumn("explodedData" , explode(col("data.insured_entities")))

    explodateData.printSchema()*/

    /**
     * size of breeds by sanitized policy
     * */
    /*    val numberofPetsByPolicy = policyData.withColumn("petsSize", size(col("data.insured_entities")))

    numberofPetsByPolicy.show()*/

    //Average number of breeds by group
   /* val breedbypolicy = policyData.withColumn("petsSize", size(col("data.insured_entities")))
    .groupBy("uuid").agg(avg("petsSize").as("totalpets"))
    breedbypolicy.show*/

    /*policyData.createOrReplaceTempView("policy")

    val groupdbydata = policyData.sqlContext.sql("SELECT distinct(data.insured_entities.breed) from policy ")

    groupdbydata.show*/

    policyData.createOrReplaceTempView("policy")


    /**
     *
     * */
    /*print(policyData.sqlContext.sql("Select explode(data.insured_entities.breed) AS breed from policy").schema)

    val breedbyuuid =  policyData.sqlContext.sql("Select explode(data.insured_entities.breed) AS breed,explode(data.insured_entities.value) AS value  from policy")

    //breedbyuuid.groupBy("breed").agg(count(col("breed")).as("breedcount")).sort(desc("breedcount")).show()
    breedbyuuid.groupBy("breed").count().sort(desc("count")).show()
*/

/*
    val breedbyuuid = policyData.sqlContext.sql("Select explode(arrays_zip(data.insured_entities.breed, data.insured_entities.value)) as breeds from policy")

    println(breedbyuuid.schema)
    breedbyuuid.select(col("breeds.0").alias("breed"), col("breeds.1").alias("breedclaimvalue")).groupBy("breed")
      .agg(avg("breedclaimvalue")).show()*/

    ManyPetsTasksService.AvgClaimValueByBreed(policyData, claimsData).groupBy("breed").avg("payout").show()
  }
}
