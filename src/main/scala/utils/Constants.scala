package org.manypets.cam
package utils

import config.DataConfig

object Constants {

  val config = DataConfig.getConfig()

  val sparkMasterUrl = config.getString("spark.masterurl")
  val sparkAppName = config.getString("spark.appName")
  val checkPointDir = config.getString("spark.checkpointDir")
  val batchDuration = config.getInt("spark.batchDuration")

}
