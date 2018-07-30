package co.com.psl.sss

import scala.collection.JavaConverters._

import com.twitter.finagle.{Service, http}
import com.twitter.util.Future

/**
  *
  */
class SparkServerService extends Service[http.Request, http.Response] {

  /**
    *
    * @param request
    * @return
    */
  def getParams(request : http.Request) : Seq[(String, String)] = {

    for (param_name <- request.getParamNames().asScala.toSeq)
      yield (param_name, request.getParam(param_name))
  }

  /**
    *
    * @param request
    * @return
    */
  override def apply(request: http.Request): Future[http.Response] = {

    request.path match {
      case "/init" => processInit(request)
      case "/shutdown" => processShutdown(request)
      case "/killjob" => processKillJob(request)
      case "/queryjob" => processQueryJob(request)
      case "/pi" => processPI(request)
      case _ => Future.value(http.Response(request.version, http.Status.NotFound))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  def processInit(request : http.Request) : Future[http.Response] = {

    val spark_conf = getParams(request)
    SparkServerImpl.init(spark_conf)
      // Register other Jobs Here!
    SparkServerImpl.registerJob(classOf[MonteCarloPI], new MonteCarloPIBuilder)
    Future.value(http.Response(request.version, http.Status.Ok))
  }

  /**
    *
    * @param request
    * @return
    */
  def processShutdown(request : http.Request) : Future[http.Response] = {

    SparkServerImpl.shutdown()
    Future.value(http.Response(request.version, http.Status.Ok))
  }

  def processKillJob(request : http.Request) : Future[http.Response] = {

    val job_id = request.getParam("id")
    SparkServerImpl.killJob(job_id.toInt)
    Future.value(http.Response(request.version, http.Status.Ok))
  }

  def processQueryJob(request : http.Request) : Future[http.Response] = {

    val job_id = request.getParam("id").toInt
    var str = ""

    for (j <- SparkServerImpl.queryJob(job_id)) {
      str = "\tID: "+str+j.jobId()+" Status: "+j.status().toString+"\n"
    }
    val response = http.Response(request.version, http.Status.Ok)
    response.write("Server Job ID: " + job_id + "\n\tNative Spark Jobs: \n"+str)
    Future.value(response)
  }

  /**
    *
    * @param request
    * @return
    */
  def processPI(request : http.Request) : Future[http.Response] = {

    val pi_params = getParams(request)
    val job_ID = SparkServerImpl.createJob(classOf[MonteCarloPI], None, pi_params)
    val response = http.Response(request.version, http.Status.Ok)
    response.write("Server Job ID: " + job_ID)

    Future.value(response)
  }
}
