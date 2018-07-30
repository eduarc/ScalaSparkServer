import co.com.psl.sss.SparkServerService
import com.twitter.finagle._
import com.twitter.util._

object SparkServerMain extends App {

  val my_service = new SparkServerService()
  val server = Http.serve(":8080", my_service)

  Await.ready(server)
}

