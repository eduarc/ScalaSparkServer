import co.com.psl.sss.{SparkServerImpl, SparkServerService}
import com.twitter.finagle.Http
import com.twitter.util.Await

object SparkServerMain extends App {

  val myService = new SparkServerService(new SparkServerImpl)
  val server = Http.serve(":8080", myService)

  Await.ready(server)
}
