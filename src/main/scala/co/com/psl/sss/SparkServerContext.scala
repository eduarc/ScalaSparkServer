package co.com.psl.sss

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  *
  */
trait SparkServerContext {

  /**
    *
    */
  val spark_config : SparkConf

  /**
    *
    */
  val spark_session : SparkSession

  /**
    *
    */
  val spark_context : SparkContext

}
