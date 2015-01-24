/*
 * Copyright (c) 2014. Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.twitter.cassovary.graph
import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestGraphSpec extends WordSpec with Matchers with GraphBehaviours {

  "three node graph g3 stored in both directions" should {
    val graph = TestGraphs.g3

    "satisfy expected basic graph properties" in {
      graph.isDirStored(GraphDir.InDir) should be (true)
      graph.isDirStored(GraphDir.OutDir) should be (true)
      graph.nodeCount should be (3)
      graph.maxNodeId should be (12)
      graph.edgeCount should be (4L)
    }
  }

  "six node graph stored in only out direction" should {
    val graph = TestGraphs.g6_onlyout

    "satisfy expected basic graph properties" in {
      graph.isDirStored(GraphDir.InDir) should be (false)
      graph.isDirStored(GraphDir.OutDir) should be (true)
      graph.nodeCount should be (6)
      graph.maxNodeId should be (15)
      graph.edgeCount should be (11)
    }
  }

  "a complete graph" when {
    "with 20 nodes" should {
      val n = 20
      val graph = TestGraphs.generateCompleteGraph(n)
      behave like completeGraph(graph, n)
    }
  }

  "a random graph" when {
    "probability of edge is 0 and numNodes is 10" should {
      val n = 10
      val graph = TestGraphs.generateRandomGraph(n, 0.0)
      "have correct number of nodes and edges" in {
        graph.nodeCount should be(n)
        graph.edgeCount should be(0)
      }
    }

    "probability of edge is 1 and numNodes is 10" should {
      val n = 10
      val graph = TestGraphs.generateRandomGraph(n, 1.0)
      behave like completeGraph(graph, n)
    }

    "probability of edge is 0.5 and numNodes is 10" should {
      val n = 10
      val d = 5
      val graph = TestGraphs.generateRandomGraph(n, TestGraphs.getProbEdgeRandomDirected(n, d))
      behave like correctDirectedGraph(graph, n)
    }
  }

  "a random undirected graph" when {
    "probability of edge is 0 and numNodes is 10" should {
      val n = 10
      val graph = TestGraphs.generateRandomUndirectedGraph(n, 0.0)
      "have correct number of nodes and edges" in {
        graph.nodeCount should be(n)
        graph.edgeCount should be(0)
      }
    }

    "probability of edge is 1 and numNodes is 10" should {
      val n = 10
      val graph = TestGraphs.generateRandomUndirectedGraph(n, 1.0)
      behave like completeGraph(graph, n)
    }

    "probability of edge is 0.3 and numNodes is 15" should {
      val n = 15
      val graph = TestGraphs.generateRandomUndirectedGraph(n, 0.3)
      behave like correctUndirectedGraph(graph, n)
    }

    "probability of edge is 0.3 and numNodes is 16" should {
      val n = 16
      val graph = TestGraphs.generateRandomUndirectedGraph(n, 0.3)
      behave like correctUndirectedGraph(graph, n)
    }
  }
}
