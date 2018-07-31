package co.com.psl.sss

import org.apache.spark.{SparkConf, SparkContext, SparkJobInfo}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashMap

class SparkServerImpl extends SparkServer {

  var serverStatus = SparkServer.Status.CREATED
  var sparkServerContext : Option[SparkServerContext] = None
  val registeredJobBuilders : HashMap[String, SparkJobBuilder] = HashMap()
  private var jobPool : Option[SparkJobPool] = None

  /**
    *
    * @param config
    */
  override def init(config : Traversable[(String, String)]): Unit = synchronized {

    if (serverStatus == SparkServer.Status.RUNNING) {
      throw new IllegalStateException("Server must be not running or finished")
    }
    sparkServerContext = Option(new SparkServerContext {
      override val sparkConfig: SparkConf = new SparkConf()
      sparkConfig.setAll(config)
      override val sparkSession: SparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()
      override val sparkContext: SparkContext = sparkSession.sparkContext
    })
    jobPool = Option(new SparkJobPoolImpl(sparkServerContext.get))
    serverStatus = SparkServer.Status.RUNNING
  }

  /**
    *
    */
  override def shutdown(): Unit = synchronized {

    if (serverStatus != SparkServer.Status.RUNNING) {
      throw new IllegalStateException("Server must be running to be finished")
    }
    jobPool.get.getAllJobsIDs().foreach(killJob)
    sparkServerContext.get.sparkSession.stop()
    jobPool = None
    sparkServerContext = None
    serverStatus = SparkServer.Status.FINISHED
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

    val builder = registeredJobBuilders(jobUniqueName)
    val new_job = builder.configs(configs).params(args).build()
    jobPool match {
      case Some(x) => x.start(new_job)
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
  override def status: Int = serverStatus
}
