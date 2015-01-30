package com.twitter.cassovary.algorithms.centrality

import com.twitter.cassovary.graph._
import org.scalatest.{Matchers, WordSpec}

/**
 * Created by bmckown on 1/28/15.
 */
class DegreeCentralitySpec extends WordSpec with Matchers {

  "Degree Centrality" should {

    lazy val graph  = TestGraphs.g6WithEmptyNodes

    lazy val unnormalizedInDegreeCentrality = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0, 3.0, 1.0, 3.0, 1.0)
    lazy val unnormalizedOutDegreeCentrality = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3.0, 2.0, 1.0, 2.0, 1.0, 2.0)

    "return in-degree centrality values that are less than one for each node" in {
      val centrality = InDegreeCentrality(graph)
      centrality shouldEqual unnormalizedInDegreeCentrality.map(c => c / (graph.nodeCount - 1))
    }

    "return out-degree centrality values that are less than one for each node" in {
      val centrality = OutDegreeCentrality(graph)
      centrality shouldEqual unnormalizedOutDegreeCentrality.map(c => c / (graph.nodeCount - 1))
    }

    "return in-degree centrality values that are greater than or equal to zero with integer values" in {
      val centrality = InDegreeCentrality(graph, false)
      centrality shouldEqual unnormalizedInDegreeCentrality
    }

    "return out-degree centrality values that are greater than or equal to zero with integer values" in {
      val centrality = OutDegreeCentrality(graph, false)
      centrality shouldEqual unnormalizedOutDegreeCentrality
    }
  }
}
