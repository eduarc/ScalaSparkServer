package co.com.psl.sss

import org.scalatest.FlatSpec

class SparkServerSpec extends FlatSpec {

  trait FixtureSparkServer {
    val sparkServer = new SparkServerImpl
    val originalConfigs = Array(("spark.master", "local"), ("spark.app.name", "SparkServer"))
  }

  "The SparkServer.init method" should "set correctly the configurations passed by parameters" in
    new FixtureSparkServer {

    sparkServer.init(originalConfigs)
    val returnedConfigs = Set(sparkServer.sparkServerContext.get.sparkContext.getConf.getAll.toSeq:_*)

    for (conf <- originalConfigs) {
      assert(returnedConfigs.contains(conf))
    }
  }

  "The SparkServer.init method" should " throws an exception if the server is already running" in
    new FixtureSparkServer {

    sparkServer.init(originalConfigs)

    assertThrows[IllegalStateException] {
      sparkServer.init(originalConfigs)
    }
  }

  "The SparkServer.shutdown method" should " throws an IllegalStateException if the server is not running" in
    new FixtureSparkServer {

    assertThrows[IllegalStateException] {
      sparkServer.shutdown()
    }
  }

  "The SparkServer.createJob method" should "throws a IllegalStateException if the server is not running" in
    new FixtureSparkServer {

    sparkServer.registerJob("pi", new MonteCarloPIBuilder)

    assertThrows[IllegalStateException] {
      sparkServer.createJob("pi", None, Array(("n", "10000")))
    }
  }

  "The SparkServer.createJob method" should " throws a NoSuchElementException if the job is not registered" in
    new FixtureSparkServer {

    sparkServer.init(originalConfigs)

    assertThrows[NoSuchElementException] {
      sparkServer.createJob("pi", None, Array(("n", "10000")))
    }
  }

  "The SparkServer.createJob method" should " create the job is it is registered and the server is running" in
    new FixtureSparkServer {

      sparkServer.init(originalConfigs)
      sparkServer.registerJob("pi", new MonteCarloPIBuilder)
      sparkServer.createJob("pi", None, Array(("n", "10000")))
      succeed
    }
}
