package org.manypets.cam

import com.holdenkarau.spark.testing.SharedSparkContext
import org.manypets.cam.config.DataConfig
import org.manypets.cam.service.{QuantexxaService, ReadFiles}
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class QuantexxaTests extends FunSuite with SharedSparkContext with Checkers {

  val dataConfig = DataConfig.getConfig()

  def before(): Unit = {}

  test("Testing number of flights n 6th month") {
    val flightsDf =
      ReadFiles.readCSVFile(Option(
        "/home/gaur/Downloads/Flight_Data_Assignment/Flight Data Assignment/flightData.csv"))
    QuantexxaService.getMonthlyFlights(flightsDf).filter()
  }

}
