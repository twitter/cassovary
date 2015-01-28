package com.twitter.cassovary.algorithms.centrality

import com.twitter.cassovary.graph.TestGraphs
import org.scalatest.{Matchers, WordSpec}

/**
 * Created by bmckown on 1/28/15.
 */
class DegreeCentralitySpec extends WordSpec with Matchers {

  "Degree Centrality" should {

    lazy val graph  = TestGraphs.g6

    "Return a zero indegree centrality for graphs with only out edges" in {
      val centrality = InDegreeCentrality.apply(graph)
      val unnormalizedCentrality = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0, 3.0, 1.0, 3.0, 1.0)

      centrality shouldEqual unnormalizedCentrality.map(c => c / (graph.maxNodeId - 1))
    }

    "Return a non-zero outdegree centrality for graphs with only out edges" in {
      val centrality = OutDegreeCentrality.apply(graph)
      val unnormalizedCentrality = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3.0, 2.0, 1.0, 2.0, 1.0, 2.0)

      centrality shouldEqual unnormalizedCentrality.map(c => c / (graph.maxNodeId - 1))
    }
  }
}
