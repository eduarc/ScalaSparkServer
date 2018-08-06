package co.com.psl.sss

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkJobInfo

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
/**
  *
  * @param jobId
  * @param name
  * @param jobThread
  */
case class SparkJobUnit(val jobId : Int, val name : String, val jobFuture : Future[Unit])

/**
  *
  * @param sparkServerContext
  */
class SparkJobPoolImpl(val sparkServerContext : SparkServerContext) extends SparkJobPool {

  val pool = TrieMap[Int, SparkJobUnit]()
  val nextJobId = new AtomicInteger

  /**
    *
    * @param sparkJob
    * @param sync
    * @return
    */
  override def start(sparkJob: SparkJob, sync : Boolean = false)(implicit ec: ExecutionContext): Int = {

    val jobId = nextJobId.getAndIncrement()
    val jobFuture = Future[Unit] {
      sparkServerContext.sparkContext.setJobGroup(jobId.toString, "", true)
      sparkJob.main(sparkServerContext.sparkSession)
    }
    val jobUnit = SparkJobUnit(jobId, sparkJob.name, jobFuture)
    pool.put(jobId, jobUnit)

    if (sync) {
      Await.ready(jobFuture, ((1L<<63)-1).nanoseconds)
    }
    jobId
  }

  /**
    *
    * @param jobId
    */
  override def stop(jobId: Int): Unit = {

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
