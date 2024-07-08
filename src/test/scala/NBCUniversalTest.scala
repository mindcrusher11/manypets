package org.manypets.cam

import config.DataConfig
import utils.{NBCConstants, QuantexxaConstants}

import com.holdenkarau.spark.testing.SharedSparkContext
import service.NBCUniversalService.{getDriverAvgAndFastTime, getF1DataFrame}

import com.typesafe.config.Config
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class NBCUniversalTest extends FunSuite with SharedSparkContext with Checkers {

  val dataConfig: Config = DataConfig.getConfig()

  def before(): Unit = {}

  test("Testing number of flights in 3rd month") {

    val flightsDf =
      getDriverAvgAndFastTime(getF1DataFrame(""))

    println(flightsDf.select(NBCConstants.avgTime).head()(0))

    val property = flightsDf.select(NBCConstants.avgTime).head()(0) === 1.24

    check(property)
  }

}
