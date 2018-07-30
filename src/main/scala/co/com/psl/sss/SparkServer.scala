package co.com.psl.sss

import org.apache.spark.SparkJobInfo

/**
  *
  */
object SparkServer {

  object Status {
    val CREATED : Int = 0
    val RUNNING : Int = 1
    val FINISHED : Int = 2
  }
}

/**
  *
  */
trait SparkServer {

  /**
    *
    * @param config
    */
  def init(config : Traversable[(String, String)]) : Unit

  /**
    *
    */
  def shutdown() : Unit

  /**
    *
    * @return
    */
  def getContext() : SparkServerContext

  /**
    *
    * @param classType
    * @param builder
    * @return
    */
  def registerJob(classType : Class[_], builder: SparkJobBuilder) : SparkServer

  /**
    *
    * @param classType
    * @param configs
    * @param args
    * @return
    */
  def createJob(classType : Class[_],
                configs : Iterable[(String, String)],
                args : Iterable[(String, String)]): Int

  /**
    *
    * @param job_ID
    * @return
    */
  def killJob(job_ID : Int) : SparkServer

  /**
    *
    * @param job_ID
    * @return
    */
  def queryJob(job_ID : Int) : Seq[SparkJobInfo]
}
