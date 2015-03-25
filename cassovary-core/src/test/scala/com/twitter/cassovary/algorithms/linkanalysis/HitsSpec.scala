package com.twitter.cassovary.algorithms.linkanalysis

import com.twitter.cassovary.graph.TestGraphs
import org.scalatest.{Matchers, WordSpec}

class HitsSpec extends WordSpec with Matchers {
  lazy val graph = TestGraphs.g6
  lazy val graphInOnly = TestGraphs.g6_onlyin

  def hitsTestHelper(finalIter: HitsIterationState, hubsTarget: Array[Double], authoritiesTarget: Array[Double]) = {
    (finalIter.hubs zip hubsTarget) foreach { case(h, t) => h should be (t +- 0.0005) }
    (finalIter.authorities zip authoritiesTarget) foreach { case(a, t) => a should be (t +- 0.0005) }
  }

  "Hits" should {

    "return initial values when zero iterations are requested" in {
      val hits = new Hits(graph, HitsParams(Some(0), normalize=true))
      val finalIter = hits.run()

      finalIter.error shouldEqual 100 + 1e-8
      finalIter.authorities foreach { a => a shouldEqual 0.0 }

      finalIter.hubs.zipWithIndex.foreach { case(v,n) =>
        v should be ((if (graph.existsNodeId(n)) 1.0 / graph.nodeCount else 0.0) +- .0000005)
      }
    }

    "return proper values when unnormalized" in {
      val hits = new Hits(graph, HitsParams(normalize=false))
      val finalIter = hits.run()

      val hubsTarget = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        0.9484, 1.0000, 0.4543, 1.0000, 0.0000, 0.2787)
      val authTarget = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        0.0945, 0.4162, 1.0000, 0.3217, 0.8324, 0.0000)

      finalIter.error should be < 1.0e-8
      finalIter.iteration shouldEqual 30
      hitsTestHelper(finalIter, hubsTarget, authTarget)
    }

    "return proper values when normalized" in {
      val hits = new Hits(graph, HitsParams(normalize=true))
      val finalIter = hits.run()

      val hubsTarget = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        0.2576, 0.2716, 0.1234, 0.2716, 0.0000, 0.0757)
      val authTarget = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        0.0355, 0.1562, 0.3753, 0.1207, 0.3123, 0.0000)

      finalIter.error should be < 1.0e-8
      finalIter.iteration shouldEqual 30
      hitsTestHelper(finalIter, hubsTarget, authTarget)
    }

    "return proper values when a graph is stored OnlyIn" in {
      val hits = new Hits(graphInOnly, HitsParams(normalize = true))
      val finalIter = hits.run()

      val hubsTarget = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        0.0355, 0.1562, 0.3753, 0.1207, 0.3123, 0.0000)
      val authTarget = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        0.2576, 0.2716, 0.1234, 0.2716, 0.0000, 0.0757)

      finalIter.error should be < 1.0e-8
      finalIter.iteration shouldEqual 32
      hitsTestHelper(finalIter, hubsTarget, authTarget)
    }

    "stop at max number of iterations" in {
      val hits = new Hits(graph, HitsParams(Some(10), normalize=true))
      val finalIter = hits.run()

      finalIter.iteration shouldEqual 10
      finalIter.error should be > 1e-8
    }

    "converge if max iterations is None" in {
      val hits = new Hits(graph, HitsParams(None, 1e-50, normalize=true))
      val finalIter = hits.run()

      finalIter.error should be < 1e-50
      finalIter.iteration shouldEqual 70
    }
  }
}
