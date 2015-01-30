package com.twitter.cassovary.algorithms.centrality

import com.twitter.cassovary.graph._
import org.scalatest.{Matchers, WordSpec}

/**
 * Created by bmckown on 1/28/15.
 */
class DegreeCentralitySpec extends WordSpec with Matchers {

  "Degree Centrality" should {

    val nodeSeqIterator = () => Seq(
      NodeIdEdgesMaxId(0, Array.empty[Int]),
      NodeIdEdgesMaxId(1, Array.empty[Int]),
      NodeIdEdgesMaxId(10, Array(11, 12, 13)),
      NodeIdEdgesMaxId(11, Array(12, 14)),
      NodeIdEdgesMaxId(12, Array(14)),
      NodeIdEdgesMaxId(13, Array(12, 14)),
      NodeIdEdgesMaxId(14, Array(15)),
      NodeIdEdgesMaxId(15, Array(10, 11))
    ).iterator

    lazy val graph  = ArrayBasedDirectedGraph(nodeSeqIterator, StoredGraphDir.BothInOut)

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
