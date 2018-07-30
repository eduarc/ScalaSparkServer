package co.com.psl.sss

import org.apache.spark.{SparkConf, SparkContext, SparkJobInfo}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashMap

object SparkServerImpl extends SparkServer {

  private var server_status = SparkServer.Status.CREATED
  private var spark_server_context: SparkServerContext = null
  private var job_pool : SparkJobPool = null
  private val registered_job_builders : HashMap[Class[_], SparkJobBuilder] = HashMap()

  /**
    *
    * @param config
    */
  override def init(config : Traversable[(String, String)]): Unit = synchronized {

    if (server_status == SparkServer.Status.RUNNING) {
      throw new IllegalStateException("Server must be not running or finished")
    }
    spark_server_context = new SparkServerContext {
      override val spark_config: SparkConf = new SparkConf()
      spark_config.setAll(config)
      override val spark_session: SparkSession = SparkSession.builder().config(spark_config).getOrCreate()
      override val spark_context: SparkContext = spark_session.sparkContext
    }
    job_pool = new SparkJobPoolImpl(spark_server_context)
    server_status = SparkServer.Status.RUNNING
  }

  /**
    *
    */
  override def shutdown(): Unit = synchronized {

    if (server_status != SparkServer.Status.RUNNING) {
      throw new IllegalStateException("Server must be running to be finished")
    }
    job_pool.getAllJobsIDs().foreach(killJob)
    spark_server_context.spark_session.stop()
    spark_server_context = null
    server_status = SparkServer.Status.FINISHED
  }

  /**
    *
    * @return
    */
  override def getContext(): SparkServerContext = {
     spark_server_context
  }

  /**
    *
    * @param classType
    * @param builder
    * @return
    */
  override def registerJob(classType : Class[_], builder : SparkJobBuilder) : SparkServer = {

    registered_job_builders(classType) = builder
    this
  }

  /**
    *
    * @param classType
    * @param configs
    * @param args
    * @return
    */
  override def createJob(classType : Class[_],
                         configs : Iterable[(String, String)],
                         args : Iterable[(String, String)]): Int = {

    val builder = registered_job_builders(classType)
    val new_job = builder.configs(configs).params(args).build()
    job_pool.start(new_job)
  }

  /**
    *
    * @param job_ID
    * @return
    */
  override def killJob(job_ID: Int): SparkServer = {

    job_pool.stop(job_ID)
    this
  }

  /**
    *
    * @param job_ID
    */
  override def queryJob(job_ID: Int): Seq[SparkJobInfo] = {
    job_pool.getNativeSparkJobs(job_ID)
  }
}
