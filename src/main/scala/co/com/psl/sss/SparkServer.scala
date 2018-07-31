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
  def registerJob(jobUniqueName : String, builder : SparkJobBuilder) : SparkServer

  /**
    *
    * @param jobUniqueName
    * @param configs
    * @param args
    * @return
    */
  def createJob(jobUniqueName : String,
                configs : Iterable[(String, String)],
                args : Iterable[(String, String)]): Int

  /**
    *
    * @param jobId
    * @return
    */
  def killJob(jobId : Int) : SparkServer

  /**
    *
    * @param jobId
    * @return
    */
  def queryJob(jobId : Int) : Seq[SparkJobInfo]

  /**
    *
    * @return
    */
  def status : Int
}
