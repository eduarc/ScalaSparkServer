package co.com.psl.sss

trait SparkJobBuilder {

  /**
    *
    * @param configs
    * @return
    */
  def configs(configs : Iterable[(String, String)]) : SparkJobBuilder

  /**
    *
    * @param args
    * @return
    */
  def params(args : Iterable[(String, String)]) : SparkJobBuilder

  /**
    *
    * @return
    */
  def build() : SparkJob
}
