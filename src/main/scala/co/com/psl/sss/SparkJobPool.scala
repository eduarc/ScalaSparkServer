package co.com.psl.sss

import org.apache.spark.SparkJobInfo

/**
  *
  */
trait SparkJobPool {

  /**
    *
    * @param sparkJob
    * @param sync
    * @return
    */
  def start(sparkJob : SparkJob, sync : Boolean = false) : Int

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
