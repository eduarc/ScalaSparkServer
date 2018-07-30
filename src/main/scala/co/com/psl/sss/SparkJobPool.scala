package co.com.psl.sss

import org.apache.spark.SparkJobInfo

/**
  *
  */
trait SparkJobPool {

  /**
    *
    * @param sparkJob
    * @return
    */
  def start(sparkJob : SparkJob) : Int

  /**
    *
    * @param sparkJobId
    */
  def stop(sparkJobId : Int) : Unit

  /**
    *
    * @return
    */
  def getAllJobsIDs() : Seq[Int]

  /**
    *
    * @param jobId
    * @return
    */
  def getNativeSparkJobs(jobId : Int) : Seq[SparkJobInfo]
}
