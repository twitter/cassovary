package com.twitter.cassovary.algorithms.linkanalysis

import com.twitter.cassovary.graph.TestGraphs
import org.scalatest.{Matchers, WordSpec}

class HitsSpec extends WordSpec with Matchers {
  lazy val graph = TestGraphs.g6

  "Hits" should {

    "return initial values when zero iterations are requested" in {
      val hits = new Hits(graph, HitsParams(Some(0), 1.0E-8, normalize=true))
      val finalIter = hits.run()

      finalIter.error shouldEqual 100 + 1e-8
      finalIter.authorities foreach { a => a shouldEqual 0.0 }

      finalIter.hubs.zipWithIndex.foreach { case(v,n) =>
        v should be ((if (graph.existsNodeId(n)) 1.0 / graph.nodeCount else 0.0) +- .0000005)
      }
    }

    "return proper values when unnormalized" in {
      val hits = new Hits(graph, HitsParams(Some(100), 1e-8, normalize=false))
      val finalIter = hits.run()

      val hubs        = finalIter.hubs
      val authorities = finalIter.authorities

      finalIter.error should be < 1.0e-8

      hubs(10) should be (0.9484 +- 0.0005)
      hubs(11) should be (1.0000 +- 0.0005)
      hubs(12) should be (0.4543 +- 0.0005)
      hubs(13) should be (1.0000 +- 0.0005)
      hubs(14) should be (0.0000 +- 0.0001)
      hubs(15) should be (0.2787 +- 0.0005)

      finalIter.iteration shouldEqual 30

      authorities(10) should be (0.0945 +- 0.0005)
      authorities(11) should be (0.4162 +- 0.0005)
      authorities(12) should be (1.0000 +- 0.0005)
      authorities(13) should be (0.3217 +- 0.0005)
      authorities(14) should be (0.8324 +- 0.0005)
      authorities(15) should be (0.0000 +- 0.0001)
    }

    "return proper values when normalized" in {
      val hits = new Hits(graph, HitsParams(Some(100), 1e-8, normalize=true))
      val finalIter = hits.run()

      val hubs        = finalIter.hubs
      val authorities = finalIter.authorities
      finalIter.error should be < 1.0e-8

      hubs(10) should be (0.2576 +- 0.0005)
      hubs(11) should be (0.2716 +- 0.0005)
      hubs(12) should be (0.1234 +- 0.0005)
      hubs(13) should be (0.2716 +- 0.0005)
      hubs(14) should be (0.0000 +- 0.0001)
      hubs(15) should be (0.0757 +- 0.0005)

      finalIter.iteration shouldEqual 30

      authorities(10) should be (0.0355 +- 0.0005)
      authorities(11) should be (0.1562 +- 0.0005)
      authorities(12) should be (0.3753 +- 0.0005)
      authorities(13) should be (0.1207 +- 0.0005)
      authorities(14) should be (0.3123 +- 0.0005)
      authorities(15) should be (0.0000 +- 0.0001)
    }

    "stop at max number of iterations" in {
      val hits = new Hits(graph, HitsParams(Some(10), 1e-8, normalize=true))
      val finalIter = hits.run()

      finalIter.iteration shouldEqual 10
      finalIter.error should be > 1e-8
    }

//    "converge if max iterations is None" in {
//      val hits = new Hits(graph, HitsParams(None, 1e-50, normalize=true))
//      val finalIter = hits.run()
//
//      finalIter.error should be < 1e-50
//      finalIter.iteration shouldEqual 70
//    }
  }
}
