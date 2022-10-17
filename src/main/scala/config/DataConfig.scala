package org.manypets.cam
package config

import org.manypets.cam.iservice.TConfig
import com.typesafe.config.{Config, ConfigFactory}

import java.io.File

/**
  * Class for loading configurations from application.conf file
  *
  * Variables and other cluster information
  *
  * @author Gaurhari
  *
  * */
object DataConfig extends TConfig {

  private var conf: Config = null

  /*
   * A function to load config values from config file
   *
   * @param filePath the path of the config file which is default to application.conf in resources folder
   *
   * @return conf as Config object of typesafe
   * */
  def getConfig(filePath: String = "application.conf"): Config = {
    if (conf == null)
      if (filePath.contains("/"))
        conf = ConfigFactory.parseFile(
          new File(filePath)
        )
      else conf = ConfigFactory.load(filePath)
    conf
  }

}

///home/gaur/manypets/src/main/resources/
