package com.twitter.cassovary.algorithms.centrality

import com.twitter.cassovary.graph._
import org.scalatest.{Matchers, WordSpec}

/**
 * Created by bmckown on 1/28/15.
 */
class DegreeCentralitySpec extends WordSpec with Matchers {

  "Degree Centrality" should {

    lazy val graph = TestGraphs.g6WithEmptyNodes

    "return in-degree centrality values that are less than one for each node" in {
      val idc = new DegreeCentrality(graph, GraphDir.InDir, true)
      val centrality = idc.centralityValues
      centrality(0)  shouldEqual 0.0
      centrality(1)  shouldEqual 0.0
      centrality(11) shouldEqual 0.2857142857142857
    }

    "return out-degree centrality values that are integer valued" in {
      val odc = new DegreeCentrality(graph, GraphDir.OutDir, false)
      val centrality = odc.centralityValues
      centrality(0)  shouldEqual 0.0
      centrality(1)  shouldEqual 0.0
      centrality(11) shouldEqual 2.0
    }
  }
}
