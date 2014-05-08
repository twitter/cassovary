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
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class TestGraphSpec extends WordSpec with ShouldMatchers {

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

  "a complete graph" should {
    " (with 20 nodes) satisfy basic checks" in {
      val n = 20
      val graph = TestGraphs.generateCompleteGraph(n)
      graph.nodeCount should be (n)
      graph.edgeCount should be (n * (n - 1))
      graph foreach { node =>
        val neighbors = (1 to n) filter { _ != node.id }
        node.inboundNodes() should be(neighbors)
        node.outboundNodes() should be(neighbors)
      }
    }
  }

  "a random graph" should {
    "(with 10 nodes) satisfy basic checks" in {
      val n = 10
      val d = 5
      val graph = TestGraphs.generateRandomGraph(n, d)
      graph.nodeCount should be (n)
      graph.edgeCount should be <= (n * (n - 1).toLong)
      graph foreach { node =>
        (0 to 1) foreach { dir =>
          val neighbors = if (dir == 0) node.outboundNodes() else node.inboundNodes()
          // should not have duplicates
          neighbors.size should be (neighbors.toSet.size)
          neighbors foreach { nbr =>
            nbr should be >= 0
            nbr should be < n
            nbr should not be node.id
            val neighborNode = graph.getNodeById(nbr).get
            val neighborNodeOtherDirEdges =
              if (dir == 0) neighborNode.inboundNodes()
              else neighborNode.outboundNodes()
            neighborNodeOtherDirEdges should contain (node.id)
          }
        }
      }
    }
  }

  "a random undirected graph" should {
    "(with 15 nodes) satisfy basic checks" in {
      val n = 15
      val graph = TestGraphs.generateRandomUndirectedGraph(n, 0.3)
      graph.nodeCount should be (n)
      graph.edgeCount should be <= (n * (n - 1).toLong)
      graph foreach { node =>
        node.inboundNodes().sorted should equal (node.outboundNodes().sorted)
        node.inboundNodes().size should equal (node.inboundNodes().toSet.size)
        node.inboundNodes() foreach { nbr =>
          nbr should be >= 0
          nbr should be < n
          nbr should not be node.id
          val neighborNode = graph.getNodeById(nbr).get
          neighborNode.outboundNodes() should contain (node.id)
        }
      }
    }
  }

}
