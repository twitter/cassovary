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
package com.twitter.cassovary.algorithms

import com.twitter.cassovary.graph.TestGraphs
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

class TriangleCountSpec extends WordSpec with ShouldMatchers {

  def averageOfPairs(f: => (Double, Double), repetitions: Int): (Double, Double) = {
    val sums = (0 until repetitions).map(_ => f).reduce((p1, p2) => (p1._1 + p2._1, p1._2 + p2._2))
    (sums._1 / repetitions, sums._2 / repetitions)
  }

  "Triangle count" should {

    "compute wedges in edge reservoir" in {
      val edgeReservoir = new IntTuplesArray(10, 2)
      edgeReservoir.set(0, Seq(0, 1))
      edgeReservoir.set(1, Seq(1, 2))
      edgeReservoir.set(2, Seq(2, 3))
      val tc = new TriangleCount(TestGraphs.generateCompleteGraph(4), TriangleCountParameters(10, 10))
      tc.computeWedgesInEdgeReservoir(edgeReservoir) should equal(2)

      edgeReservoir.set(3, Seq(1, 2))
      tc.computeWedgesInEdgeReservoir(edgeReservoir) should equal(2)

      edgeReservoir.set(4, Seq(0, 2))
      edgeReservoir.set(5, Seq(7, 8))
      tc.computeWedgesInEdgeReservoir(edgeReservoir) should equal(5)
    }

    "Return correct results for a graph with no triangles" in {
      val numberOfNodes = 1000
      val graph = TestGraphs.generateRandomUndirectedGraph(numberOfNodes, 2.0 / numberOfNodes)
      val pars = TriangleCountParameters(200, 200)
      val (transitivity, triangles) = TriangleCount(graph, pars)
      transitivity should be(0.0 plusOrMinus 0.05)

      triangles should be(0.0 plusOrMinus 20.0)
    }


    "Return correct results for Erdos-Renly mutual graphs" in {
      val edgeProbability = 0.3
      val numberOfNodes = 200
      val erGraph = TestGraphs.generateRandomUndirectedGraph(numberOfNodes, edgeProbability)
      val pars = TriangleCountParameters(500, 500)
      val (transitivity, triangles) = averageOfPairs(TriangleCount(erGraph, pars), 10)
      transitivity should be(edgeProbability plusOrMinus 0.15 * edgeProbability)

      def averageTrianglesInERGraph(nodes: Int, p: Double) = {
        p * p * p * nodes * (nodes - 1) * (nodes - 2) / 6
      }
      val expectedTriangles = averageTrianglesInERGraph(numberOfNodes, edgeProbability)
      triangles should be(expectedTriangles plusOrMinus (0.3 * expectedTriangles))
    }

    "Return correct results for complete graph" in {
      val nodes = 100
      val graph = TestGraphs.generateCompleteGraph(nodes)
      val pars = TriangleCountParameters(1000, 1000)
      val (transitivity, triangles) = averageOfPairs(TriangleCount(graph, pars), 5)
      transitivity should be(1.0 plusOrMinus 0.1)

      def trianglesInCompleteGraph(nodes: Int): Double = {
        (nodes * (nodes - 1) * (nodes - 2)) / 6
      }

      val expectedTriangles = trianglesInCompleteGraph(nodes)
      triangles should be(expectedTriangles plusOrMinus (0.25 * expectedTriangles))
    }
  }
}
