package co.com.psl.sss

import org.apache.spark.SparkJobInfo

/**
  *
  */
trait SparkJobPool {

  /**
    *
    * @param spark_job
    * @return
    */
  def start(spark_job : SparkJob) : Int

  /**
    *
    * @param spark_job_ID
    */
  def stop(spark_job_ID : Int) : Unit

  /**
    *
    * @return
    */
  def getAllJobsIDs() : Seq[Int]

  /**
    *
    * @param job_id
    * @return
    */
  def getNativeSparkJobs(job_id : Int) : Seq[SparkJobInfo]
}
