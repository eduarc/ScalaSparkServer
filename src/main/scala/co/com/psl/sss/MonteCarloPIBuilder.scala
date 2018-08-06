package co.com.psl.sss

/**
  *
  */
case class MonteCarloPIBuilder(var numPoints : Int = 0) extends SparkJobBuilder {

  /**
    *
    * @param configs
    * @return
    */
  override def configs(configs: Iterable[(String, String)]): SparkJobBuilder = {
    this.copy()
  }

  /**
    *
    * @param args
    * @return
    */
  override def params(args: Iterable[(String, String)]): SparkJobBuilder = {

    for (param <- args) param._1 match {
      case "n" => {
        numPoints = param._2.toInt
      }
      case _ => // Do nothing with extra parameters!
    }
    this.copy()
  }

  /**
    *
    * @return
    */
  override def build(): SparkJob = {
    new MonteCarloPI(numPoints)
  }
}
