package co.com.psl.sss

import org.apache.spark.SparkJobInfo

import scala.concurrent.ExecutionContext

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
  def start(sparkJob : SparkJob, sync : Boolean = false)(implicit ec: ExecutionContext) : Int

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
