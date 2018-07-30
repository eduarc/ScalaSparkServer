import co.com.psl.sss.{SparkServerImpl, SparkServerService}
import com.twitter.finagle._
import com.twitter.util._

object SparkServerMain extends App {

  val myService = new SparkServerService(SparkServerImpl)
  val server = Http.serve(":8080", myService)

  Await.ready(server)
}

