package co.com.psl.sss

import scala.collection.JavaConverters._

import com.twitter.finagle.{Service, http}
import com.twitter.util.Future

/**
  *
  */
class SparkServerService(val sparkServer : SparkServer) extends Service[http.Request, http.Response] {

  /**
    *
    * @param request
    * @return
    */
  def getParams(request : http.Request) : Seq[(String, String)] = {

    for (paramName <- request.getParamNames().asScala.toSeq)
      yield (paramName, request.getParam(paramName))
  }

  /**
    *
    * @param request
    * @return
    */
  override def apply(request: http.Request): Future[http.Response] = {

    request.path match {
      case "/init" => doInit(request)
      case "/shutdown" => doShutdown(request)
      case "/killjob" => doKillJob(request)
      case "/queryjob" => doQueryJob(request)
      case "/runjob" => doRunJob(request)
      case _ => Future.value(http.Response(request.version, http.Status.NotFound))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  def doInit(request : http.Request) : Future[http.Response] = {

    val sparkConf = getParams(request)
    sparkServer.init(sparkConf)
    // Register other Jobs Here!
    sparkServer.registerJob("pi", new MonteCarloPIBuilder)
    Future.value(http.Response(request.version, http.Status.Ok))
  }

  /**
    *
    * @param request
    * @return
    */
  def doShutdown(request : http.Request) : Future[http.Response] = {

    sparkServer.shutdown()
    Future.value(http.Response(request.version, http.Status.Ok))
  }

  def doKillJob(request : http.Request) : Future[http.Response] = {

    val jobId = request.getParam("id")
    sparkServer.killJob(jobId.toInt)
    Future.value(http.Response(request.version, http.Status.Ok))
  }

  def doQueryJob(request : http.Request) : Future[http.Response] = {

    val jobId = request.getParam("id").toInt
    var str = ""

    for (j <- sparkServer.queryJob(jobId)) {
      str += s"\tID: ${j.jobId()} Status: ${j.status()}\n"
    }
    val response = http.Response(request.version, http.Status.Ok)
    response.write(s"Server Job ID: $jobId\n\tNative Spark Jobs:\n$str")
    Future.value(response)
  }

  /**
    *
    * @param request
    * @return
    */
  def doRunJob(request : http.Request) : Future[http.Response] = {

    val jobUniqueName = request.getParam("id").toString
    val params = getParams(request)
    val jobId = sparkServer.createJob(jobUniqueName, None, params)
    val response = http.Response(request.version, http.Status.Ok)
    response.write(s"Server Job ID: $jobId")
    Future.value(response)
  }
}
