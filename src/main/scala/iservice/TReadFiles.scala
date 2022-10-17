package org.manypets.cam
package iservice

/**
 * Trait for reading files implementation
 *
 * @author Gaurhari
 *
 * */
trait TReadFiles {

  /**
  * abstract method for reading json file
  *
  * @param filePath
  * */
  def readJsonFile(filePath: Option[String]): Any

  /**
  * abstract method for reading csv file
  *
  * @param filePath
  * */
  def readCSVFile(filePath: Option[String]): Any

}
