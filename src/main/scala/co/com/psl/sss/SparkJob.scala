package co.com.psl.sss

import org.apache.spark.sql.SparkSession

/**
  *
  */
trait SparkJob {

  /**
    *
    * @return
    */
  def name() : String

  /**
    *
    * @param sparkSession
    */
  def main(sparkSession : SparkSession) : Unit
}
