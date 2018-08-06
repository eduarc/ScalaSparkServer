package co.com.psl.sss

import org.scalatest.FlatSpec

class MonteCarloPIBuilderSpec extends FlatSpec {

  "A MonterCarloPIBuilder" should "assign the attributes of instances of MonteCarloPI" in {

    val instance = new MonteCarloPIBuilder()
      .params(Array(("n", "10")).toSeq)
      .build()
      .asInstanceOf[MonteCarloPI]
    assert(instance.numPoints == 10)
  }

  "A MonterCarloPIBuilder" should "ignore extra parameters" in {

    try {
      val instance = new MonteCarloPIBuilder()
        .params(Array(("n", "12"), ("extra0", "foo"), ("extra1", "bar")).toSeq)
        .build()
        .asInstanceOf[MonteCarloPI]
      assert(instance.numPoints == 12)
    } catch {
      case _ : Exception => fail
    }
  }
}
