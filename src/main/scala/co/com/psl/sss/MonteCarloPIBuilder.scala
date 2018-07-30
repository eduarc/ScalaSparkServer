package co.com.psl.sss

/**
  *
  */
class MonteCarloPIBuilder extends SparkJobBuilder {

  /**
    *
    */
  var num_points : Int = 0

  /**
    *
    * @param configs
    * @return
    */
  override def configs(configs: Iterable[(String, String)]): SparkJobBuilder = {
    this
  }

  /**
    *
    * @param args
    * @return
    */
  override def params(args: Iterable[(String, String)]): SparkJobBuilder = {

    for (param <- args) param match {
      case ("n", _) => {
        num_points = param._2.toInt
      }
    }
    this
  }

  /**
    *
    * @return
    */
  override def build(): SparkJob = {
    new MonteCarloPI(num_points)
  }
}
