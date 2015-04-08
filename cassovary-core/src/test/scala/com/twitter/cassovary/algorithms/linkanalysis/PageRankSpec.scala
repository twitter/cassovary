/*
 * Copyright 2014 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.twitter.cassovary.algorithms.linkanalysis

import com.twitter.cassovary.graph.{Node, DirectedGraph, TestGraphs}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{Matchers, WordSpec}

class PageRankSpec extends WordSpec with Matchers {

  val EPSILON = 1e-6

  def almostEqualMap(expected: Map[Int, Double]) = new Matcher[Array[Double]] {
    def apply(left: Array[Double]) = {
      MatchResult(
        expected forall { case (i,d) => math.abs(left(i) - d) < EPSILON },
        "Mapped integers aren't equal! \nExpected: %s \nActual: %s".format(
          expected.mkString(", "), left.mkString(", ")),
        "Mapped integers are equal"
      )
    }
  }

  def testGraphHelper(graph: DirectedGraph[Node], target: Array[Double]) = {
    val params = PageRankParams(.85, None)
    val pr = new PageRank(graph, params)
    val finalIter = pr.run()

    (finalIter.pageRank zip target) foreach { case(calculated, goal) => calculated should be (goal +- 0.00005) }
  }

  lazy val graphG6 = TestGraphs.g6
  lazy val graphInOnly = TestGraphs.g6_onlyin

  "PageRank" should {

    "Return a uniform array with 0 iterations" in {
      val params = PageRankParams(0.1, Some(0))
      val pr = new PageRank(graphG6, params)
      val finalIter = pr.run()

      finalIter.iteration shouldEqual 0
      finalIter.pageRank should almostEqualMap(Map(10 -> 1.0/6, 11 -> 1.0/6, 12 -> 1.0/6, 13 -> 1.0/6, 14 -> 1.0/6, 15 -> 1.0/6))
    }

    "Return the correct values with 1 iteration" in {
      val params = PageRankParams(0.9, Some(1))
      val pr = new PageRank(graphG6, params)
      val finalIter = pr.run()

      finalIter.iteration shouldEqual 1
      finalIter.pageRank should almostEqualMap(Map(10 -> (.1/6 + .9/12), 11 -> (.1/6 + .9*(1.0/18+1.0/12)),
        12 -> (.1/6 + .9*(1.0/6+1.0/18)), 13 -> (.1/6 + .1/2), 14 -> (.1/6 + .9/3), 15 -> 1.0/6))
    }

    "At 2 iterations still sum to 1" in {
      val params = PageRankParams(0.9, Some(2))
      val pr = new PageRank(graphG6, params)
      val finalIter = pr.run()

      finalIter.iteration shouldEqual 2
      finalIter.pageRank.sum should be(1.0 +- 1.0E-8)
    }

    "converge when no max iterations is given" in {
      val target = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        0.12699, 0.16297, 0.15616, 0.06098, 0.25292, 0.23998)
      testGraphHelper(graphG6, target)
    }

    "converge when no max iterations is given with graph stored in inbound direction only" in {
      val target = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        0.19942, 0.11661, 0.09086, 0.11661, 0.23245, 0.24406)
      testGraphHelper(graphInOnly, target)
    }

    "For a complete graph, 100 iterations still maintains the same values" in {
      val graphComplete = TestGraphs.generateCompleteGraph(10)
      val params = new PageRankParams(0.9, Some(100))
      val pr = new PageRank(graphComplete, params)

      val pagerank = pr.run().pageRank
      graphComplete.foreach { n => pagerank(n.id) shouldEqual 0.1 }
    }
  }
}
