package com.twitter.cassovary.algorithms

import org.specs.Specification
import com.twitter.cassovary.graph.{DirectedGraph, TestGraphs}
import org.specs.matcher.Matcher
import scala.Predef._
import scala.Some

class PageRankSpec extends Specification {

  case class MapAlmostEquals(a: Map[Int,Double]) extends Matcher[Array[Double]]() {
    def apply(b: => Array[Double]) = (
      {
        a.foldLeft(true) { case (truth, (i, d)) => truth && b(i) - d < 0.000001 }
      },
      "Mapped integers are equal",
      "Mapped integers aren't! Expected: %s Actual: %s".format(b.deep.mkString(", "), a)
    )
  }

  "PageRank" should {

    var graphG6: DirectedGraph = TestGraphs.g6
    var graphComplete: DirectedGraph = TestGraphs.generateCompleteGraph(10)

    "Return a uniform array with 0 iterations" in {
      val params = PageRankParams(0.1, Some(0))
      val pr = PageRank(graphG6, params)
      pr must MapAlmostEquals(Map(10 -> 1.0/6, 11 -> 1.0/6, 12 -> 1.0/6, 13 -> 1.0/6, 14 -> 1.0/6, 15 -> 1.0/6))
    }

    "Return the correct values with 1 iteration" in {
      val params = PageRankParams(0.9, Some(1))
      val pr = PageRank(graphG6, params)
      pr must MapAlmostEquals(Map(10 -> (.1/6 + .9/12), 11 -> (.1/6 + .9*(1.0/18+1.0/12)),
        12 -> (.1/6 + .9*(1.0/6+1.0/18)), 13 -> (.1/6 + .1/2), 14 -> (.1/6 + .9/3), 15 -> 1.0/6))
    }

    "At 2 iterations PageRank still sums to 1" in {
      val params = PageRankParams(0.9, Some(2))
      val pr = PageRank(graphG6, params)
      pr.sum mustEqual 1.0
    }

    "For a complete graph, 100 iterations still maintains the same values" in {
      val params = PageRankParams(0.9, Some(100))
      val pr = PageRank(graphComplete, params)
      graphComplete.foreach { n => pr(n.id) mustEqual 0.1 }
    }

  }
}
