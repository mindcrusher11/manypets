package org.manypets.cam

import config.DataConfig
import models.PolicyClaim
import service.{ManyPetsTasksService, ReadFiles}

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.Encoders
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

/** Class for test cases for data.
  *
  * @author
  *   Gaurhari
  */
class ManyPetsTests extends FunSuite with SharedSparkContext with Checkers {

  val dataConfig = DataConfig.getConfig()
  def before(): Unit = {}

  test("Testing Dataframe Schema.") {

    ClaimsSchema.getClaimsSchema[PolicyClaim]()

    val claimsDF = ReadFiles.readCSVFile(
      Option(DataConfig.getConfig().getString("file.claimsPath")))

    claimsDF.printSchema()
    ClaimsSchema.getClaimsSchema[PolicyClaim]().printTreeString()

    val property =
      forAll(claimsDF) { dataframe =>
        dataframe.schema === ClaimsSchema.getClaimsSchema[PolicyClaim]()
      }

    check(property)
  }

  test("test unique policies.") {
    val policyData = ReadFiles.readJsonFile(
      Option(DataConfig.getConfig().getString("file.policiesPath")))

    val policyDataFrame = ManyPetsTasksService.getDifferentPolicies(policyData)

    val property =
      forAll(policyDataFrame) { dataframe =>
        dataframe.collect()(0)(0) === 8643
      }

    check(property)
  }

  test("testing number of policies claimed.") {

    val claimsData = ReadFiles.readCSVFile(
      Option(DataConfig.getConfig().getString("file.claimsPath")))

    val claimedpolicyDataFrame =
      ManyPetsTasksService.claimedPolicyCount(claimsData)

    val property =
      forAll(claimedpolicyDataFrame) { dataframe =>
        dataframe.collect()(0)(0) === 3174
      }

    check(property)
  }

}
