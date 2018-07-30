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
    * @param spark_session
    */
  def main(spark_session : SparkSession) : Unit
}
