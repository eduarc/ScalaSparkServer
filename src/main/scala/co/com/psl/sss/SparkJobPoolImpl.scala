package co.com.psl.sss

import org.apache.spark.SparkJobInfo
import scala.collection.mutable.HashMap

/**
  *
  * @param job_id
  * @param name
  * @param job_thread
  */
case class SparkJobUnit(val job_id : Int, val name : String, val job_thread : Thread)

/**
  *
  * @param spark_server_context
  */
class SparkJobPoolImpl(val spark_server_context : SparkServerContext) extends SparkJobPool {

  val pool = HashMap[Int, SparkJobUnit]()

  /**
    *
    * @param spark_job
    * @return
    */
  override def start(spark_job: SparkJob): Int = {

    var job_id = -1

    pool.synchronized {
      job_id = pool.size + 1

      val job_thread = new Thread {
        override def run(): Unit = {
          spark_server_context.spark_context.setJobGroup(job_id+"", "", true)
          spark_job.main(spark_server_context.spark_session)
        }
      }
      val job_unit = SparkJobUnit(job_id, spark_job.name, job_thread)
      pool.put(job_id, job_unit)
      job_thread.start()
    }
    job_id
  }

  /**
    *
    * @param job_ID
    */
  override def stop(job_ID: Int): Unit = synchronized {

    if (!pool.contains(job_ID)) {
      throw new IllegalArgumentException("Job ID not found")
    }
    spark_server_context.spark_context.cancelJobGroup(job_ID+"")
  }

  /**
    *
    * @return
    */
  override def getAllJobsIDs(): Seq[Int] = {
    pool.keySet.toSeq
  }

  /**
    *
    * @param job_ID
    * @return
    */
  override def getNativeSparkJobs(job_ID : Int): Seq[SparkJobInfo] = {

    if (!pool.contains(job_ID)) {
      throw new IllegalArgumentException("Job ID not found")
    }
    val statusTracker = spark_server_context.spark_context.statusTracker

    for ( id <- statusTracker.getJobIdsForGroup(job_ID+"").toSeq)
      yield statusTracker.getJobInfo(id).get
  }
}
