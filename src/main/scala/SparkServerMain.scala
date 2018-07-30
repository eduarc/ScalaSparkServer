import co.com.psl.sss.SparkServerService
import com.twitter.finagle._
import com.twitter.util._

object SparkServerMain extends App {

  val myService = new SparkServerService()
  val server = Http.serve(":8080", myService)

  Await.ready(server)
}

