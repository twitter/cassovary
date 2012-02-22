/*
 * Copyright 2012 Twitter, Inc.
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

import org.specs.Specification

class DirectedGraphSpec extends Specification {
  var graph: DirectedGraph = _

  val twoNodeGraph = beforeContext {
    graph = TestGraphs.g2_mutual
  }

  val sixNodeGraph = beforeContext {
    graph = TestGraphs.g6
  }

  "two node graph with each following the other" definedAs twoNodeGraph should {
    "nodeCount and edgeCount should be correct" in {
      graph.nodeCount mustEqual 2
      graph.edgeCount mustEqual 2
    }

    "maxNodeId should be correct" in {
      graph.maxNodeId mustEqual 2
    }

    "elements should return all the nodes" in {
      graph.iterator.toList.map { _.id }.sortBy (+_) mustEqual List(1, 2)
    }

    "getNodeById should work in both positive and negative cases" in {
      graph.getNodeById(1) mustEqual Some(TestNode(1, List(2), List(2)))
      graph.getNodeById(2) mustEqual Some(TestNode(2, List(1), List(1)))
      graph.getNodeById(3) mustEqual None
    }
  }

  "six node graph" definedAs sixNodeGraph should {
    "nodeCount and edgeCount should be correct" in {
      graph.nodeCount mustEqual 6
      graph.edgeCount mustEqual 11
    }

    "maxNodeId should be correct" in {
      graph.maxNodeId mustEqual 15
    }

    "elements should return all the nodes" in {
      graph.iterator.toList.map { _.id }.sortBy(+_) mustEqual List(10, 11, 12, 13, 14, 15)
    }

    "existsNodeId should work in both positive and negative cases" in {
      graph.existsNodeId(10) mustEqual true
      graph.existsNodeId(11) mustEqual true
      graph.existsNodeId(122) mustEqual false
    }
  }

}
