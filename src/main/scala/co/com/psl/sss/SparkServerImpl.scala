package co.com.psl.sss

import org.apache.spark.{SparkConf, SparkContext, SparkJobInfo}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashMap

class SparkServerImpl extends SparkServer {

  private var serverStatus = SparkServer.Status.CREATED
  private var sparkServerContext: SparkServerContext = null
  private var jobPool : SparkJobPool = null
  private val registeredJobBuilders : HashMap[String, SparkJobBuilder] = HashMap()

  /**
    *
    * @param config
    */
  override def init(config : Traversable[(String, String)]): Unit = synchronized {

    if (serverStatus == SparkServer.Status.RUNNING) {
      throw new IllegalStateException("Server must be not running or finished")
    }
    sparkServerContext = new SparkServerContext {
      override val sparkConfig: SparkConf = new SparkConf()
      sparkConfig.setAll(config)
      override val sparkSession: SparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()
      override val sparkContext: SparkContext = sparkSession.sparkContext
    }
    jobPool = new SparkJobPoolImpl(sparkServerContext)
    serverStatus = SparkServer.Status.RUNNING
  }

  /**
    *
    */
  override def shutdown(): Unit = synchronized {

    if (serverStatus != SparkServer.Status.RUNNING) {
      throw new IllegalStateException("Server must be running to be finished")
    }
    jobPool.getAllJobsIDs().foreach(killJob)
    sparkServerContext.sparkSession.stop()
    serverStatus = SparkServer.Status.FINISHED
  }

  /**
    *
    * @return
    */
  override def getContext(): SparkServerContext = {
     sparkServerContext
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

    val builder = registeredJobBuilders(jobUniqueName)
    val new_job = builder.configs(configs).params(args).build()
    jobPool.start(new_job)
  }

  /**
    *
    * @param jobId
    * @return
    */
  override def killJob(jobId: Int): SparkServer = {

    jobPool.stop(jobId)
    this
  }

  /**
    *
    * @param jobId
    */
  override def queryJob(jobId: Int): Seq[SparkJobInfo] = {
    jobPool.getNativeSparkJobs(jobId)
  }
}
