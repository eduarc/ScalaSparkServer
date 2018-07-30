package co.com.psl.sss

import org.apache.spark.sql.SparkSession

import scala.util.Random

class MonteCarloPI(val numPoints : Int) extends SparkJob {

  override def main(sparkSession: SparkSession): Unit = {

    val rdd = sparkSession.sparkContext.parallelize(Array.ofDim[Byte](numPoints))

    val positivePoints = rdd
      .map(_ => {
        val x = Random.nextFloat()
        val y = Random.nextFloat()
        if (x*x+y*y <= 1.0) 1 else 0
      })
      .reduce((a, b) => a+b)

    val approxPI = 4.0*positivePoints/numPoints
    println(approxPI)
  }

  override def name(): String = "Monte Carlo PI"
}
