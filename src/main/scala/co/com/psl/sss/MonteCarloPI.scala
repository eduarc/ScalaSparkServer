package co.com.psl.sss

import org.apache.spark.sql.SparkSession

import scala.util.Random

class MonteCarloPI(val num_points : Int) extends SparkJob {

  override def main(sparkSession: SparkSession): Unit = {

    val rdd = sparkSession.sparkContext.parallelize(Array.ofDim[Byte](num_points))

    val positive_points = rdd
      .map(_ => {
        val x = Random.nextFloat()
        val y = Random.nextFloat()
        if (x*x+y*y <= 1.0) 1 else 0
      })
      .reduce((a, b) => a+b)

    val approx_PI = positive_points/num_points.toFloat*4.0
    println(approx_PI)
  }

  override def name(): String = "Monte Carlo PI"
}
