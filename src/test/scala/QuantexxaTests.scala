package org.manypets.cam

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.functions.{col, column}
import org.manypets.cam.config.DataConfig
import org.manypets.cam.service.{QuantexxaService, ReadFiles}
import org.manypets.cam.utils.QuantexxaConstants
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class QuantexxaTests extends FunSuite with SharedSparkContext with Checkers {

  val dataConfig = DataConfig.getConfig()

  def before(): Unit = {}

  test("Testing number of flights in 3rd month") {

    val flightsDf =
      ReadFiles.readCSVFile(Option(
        "/home/gaur/Downloads/Flight_Data_Assignment/Flight Data Assignment/flightData.csv"))

    val df = QuantexxaService
      .getMonthlyFlights(flightsDf)
      .filter(col(QuantexxaConstants.monthColumn).equalTo(3))
      .select(QuantexxaConstants.countColumn)

    val property =
      forAll(df) { dataframe =>
        dataframe.collect()(0)(0) === 8200
      }

    check(property)
  }

  test()
}
