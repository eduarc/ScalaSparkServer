package co.com.psl.sss

import org.apache.spark.SparkJobInfo
import scala.collection.mutable.HashMap

/**
  *
  * @param jobId
  * @param name
  * @param jobThread
  */
case class SparkJobUnit(val jobId : Int, val name : String, val jobThread : Thread)

/**
  *
  * @param sparkServerContext
  */
class SparkJobPoolImpl(val sparkServerContext : SparkServerContext) extends SparkJobPool {

  val pool = HashMap[Int, SparkJobUnit]()

  /**
    *
    * @param sparkJob
    * @return
    */
  override def start(sparkJob: SparkJob): Int = {

    pool.synchronized {
      val jobId = pool.size + 1

      val jobThread = new Thread {
        override def run(): Unit = {
          sparkServerContext.sparkContext.setJobGroup(jobId.toString, "", true)
          sparkJob.main(sparkServerContext.sparkSession)
        }
      }
      val jobUnit = SparkJobUnit(jobId, sparkJob.name, jobThread)
      pool.put(jobId, jobUnit)
      jobThread.start()
      jobId
    }
  }

  /**
    *
    * @param jobId
    */
  override def stop(jobId: Int): Unit = synchronized {

    if (!pool.contains(jobId)) {
      throw new IllegalArgumentException("Job Id not found")
    }
    sparkServerContext.sparkContext.cancelJobGroup(jobId.toString)
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
    * @param jobId
    * @return
    */
  override def getNativeSparkJobs(jobId : Int): Seq[SparkJobInfo] = {

    if (!pool.contains(jobId)) {
      throw new IllegalArgumentException("Job ID not found")
    }
    val statusTracker = sparkServerContext.sparkContext.statusTracker

    for ( id <- statusTracker.getJobIdsForGroup(jobId.toString).toSeq)
      yield statusTracker.getJobInfo(id).get
  }
}
