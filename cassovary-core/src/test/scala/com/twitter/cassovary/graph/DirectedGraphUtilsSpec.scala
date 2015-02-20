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
package com.twitter.cassovary.graph

import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DirectedGraphUtilsSpec extends WordSpec with Matchers {

  private def utils[V <: Node](graph: DirectedGraph[V]) = {
    (graph, new DirectedGraphUtils(graph))
  }

  "DirectedGraphUtils" when {

    "two node with each following the other" should {
      val (graph, directedGraphUtils) = utils(TestGraphs.g2_mutual)
      "#mutualedges equal 1" in {
        directedGraphUtils.getNumMutualEdges shouldEqual 1L
      }
    }

    "mutual edges on six node with only one dir stored" should {
      "be correct with only out dir" in {
        val directedGraphUtils = new DirectedGraphUtils(TestGraphs.g6_onlyout)
        directedGraphUtils.getNumMutualEdges shouldEqual 0L
      }

      "be correct with only in dir" in {
        val directedGraphUtils = new DirectedGraphUtils(TestGraphs.g6_onlyin)
        directedGraphUtils.getNumMutualEdges shouldEqual 0L
      }
    }

    "mutual edges on seven node with only one dir stored" should {
      "be correct with only out dir" in {
        val directedGraphUtils = new DirectedGraphUtils(TestGraphs.g7_onlyout)
        directedGraphUtils.getNumMutualEdges shouldEqual 4L
      }

      "be correct with only in dir" in {
        val directedGraphUtils = new DirectedGraphUtils(TestGraphs.g7_onlyin)
        directedGraphUtils.getNumMutualEdges shouldEqual 4L
      }
    }

    "three node" should {
      val (graph, directedGraphUtils) = utils(TestGraphs.g3)
      "#mutualedges equal 1" in {
        directedGraphUtils.getNumMutualEdges shouldEqual 1L
      }
    }

    "six node" should {
      val (graph, directedGraphUtils) = utils(TestGraphs.g6)

      "mutual edge check" in {
        directedGraphUtils.getNumMutualEdges shouldEqual 0L
      }
    }

    "checks on complete" should {
      val graph = TestGraphs.generateCompleteGraph(10)
      val directedGraphUtils = new DirectedGraphUtils(graph)
      "mutual edge check" in {
        directedGraphUtils.getNumMutualEdges shouldEqual graph.edgeCount / 2
      }
    }
  }
}
