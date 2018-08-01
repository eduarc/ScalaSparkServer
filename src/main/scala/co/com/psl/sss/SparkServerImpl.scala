package co.com.psl.sss

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{SparkConf, SparkJobInfo}
import org.apache.spark.sql.SparkSession

import scala.collection.concurrent.TrieMap

class SparkServerImpl extends SparkServer {

  var serverStatus = new AtomicInteger(SparkServer.Status.SHUTDOWN)
  var sparkServerContext : Option[SparkServerContext] = None
  val registeredJobBuilders = new  TrieMap[String, SparkJobBuilder]
  private var jobPool : Option[SparkJobPool] = None

  /**
    * Synchronized to avoid multiple initializations
    * @param config
    */
  override def init(config : Traversable[(String, String)]): Unit = {

    if (serverStatus.compareAndSet(SparkServer.Status.SHUTDOWN, SparkServer.Status.RUNNING)) {
      throw new IllegalStateException("Server must be not running")
    }
    val sparkConfig: SparkConf = new SparkConf().setAll(config)
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()
    sparkServerContext = Option(SparkServerContext(sparkConfig, sparkSession, sparkSession.sparkContext))
    jobPool = Option(new SparkJobPoolImpl(sparkServerContext.get))
  }

  /**
    *
    */
  override def shutdown(): Unit = {

    if (!serverStatus.compareAndSet(SparkServer.Status.RUNNING, SparkServer.Status.SHUTDOWN)) {
      throw new IllegalStateException("Server must be running to be finished")
    }
    jobPool.get.getAllJobsIDs().foreach(killJob)
    sparkServerContext.get.sparkSession.stop()
    jobPool = None
    sparkServerContext = None
  }

  /**
    *
    * @return
    */
  override def getContext(): SparkServerContext = {
     sparkServerContext.get
  }

  /**
    *
    * @param classType
    * @param builder
    * @return
    */
  override def registerJob(jobUniqueName : String, builder : SparkJobBuilder) : SparkServer = {

    registeredJobBuilders(jobUniqueName) = builder
    this
  }

  /**
    *
    * @param classType
    * @param configs
    * @param args
    * @return
    */
  override def createJob(jobUniqueName : String,
                          configs : Iterable[(String, String)],
                          args : Iterable[(String, String)]): Int = {
    jobPool match {
      case Some(x) => {
        val builder = registeredJobBuilders(jobUniqueName)
        val newJob = builder.configs(configs).params(args).build()
        x.start(newJob)
      }
      case None => throw new IllegalStateException("Server must initiated before launching tasks")
    }
  }

  /**
    *
    * @param jobId
    * @return
    */
  override def killJob(jobId: Int): SparkServer = {

    jobPool.get.stop(jobId)
    this
  }

  /**
    *
    * @param jobId
    */
  override def queryJob(jobId: Int): Seq[SparkJobInfo] = {
    jobPool.get.getNativeSparkJobs(jobId)
  }

  /**
    *
    * @return
    */
  override def status: Int = serverStatus.get()
}
