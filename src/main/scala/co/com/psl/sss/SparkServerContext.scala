package co.com.psl.sss

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  *
  */
case class SparkServerContext(

  /**
    *
    */
  val sparkConfig : SparkConf,

  /**
    *
    */
  val sparkSession : SparkSession,

  /**
    *
    */
  val sparkContext : SparkContext
)
