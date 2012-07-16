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

class CachedDirectedGraphSpec extends Specification {
  var graph: DirectedGraph = _

  val iteratorFunc = () => Seq(NodeIdEdgesMaxId(1, Array(2,3,4)),
    NodeIdEdgesMaxId(2, Array(1)),
    NodeIdEdgesMaxId(5, Array(2))).iterator

  def makeGraph(dir: StoredGraphDir.StoredGraphDir) = CachedDirectedGraph(
    iteratorFunc, dir)

  def getNode(id: Int): Option[Node] = graph.getNodeById(id)

  val smallGraphOutOnly = beforeContext {
    graph = makeGraph(StoredGraphDir.OnlyOut)
  }

  val smallGraphInOnly = beforeContext {
    graph = makeGraph(StoredGraphDir.OnlyIn)
  }

  "graph containing only out edges" definedAs smallGraphOutOnly should {
    "map only out edges" in {
      getNode(1).get must DeepEqualsNode((NodeMaker(1, Array(2, 3, 4), Array())))
      getNode(2).get must DeepEqualsNode((NodeMaker(2, Array(1), Array())))
      getNode(3).get must DeepEqualsNode((NodeMaker(3, Array())))
      getNode(4).get must DeepEqualsNode((NodeMaker(4, Array())))
      getNode(5).get must DeepEqualsNode((NodeMaker(5, Array(2), Array())))
      getNode(6) mustEqual None
    }

    "iterate over all nodes" in {
      graph must DeepEqualsNodeIterable((1 to 5).flatMap(getNode(_)))
    }

    "provide the correct node count" in {
      graph.nodeCount mustBe 5
    }

    "provide the correct edge count" in {
      graph.edgeCount mustBe 5L
    }
  }

  "graph containing only in edges" definedAs smallGraphInOnly should {
    "map only in edges" in {
      getNode(1).get must DeepEqualsNode((NodeMaker(1, Array(), Array(2, 3, 4))))
      getNode(2).get must DeepEqualsNode((NodeMaker(2, Array(), Array(1))))
      getNode(3).get must DeepEqualsNode((NodeMaker(3, Array())))
      getNode(4).get must DeepEqualsNode((NodeMaker(4, Array())))
      getNode(5).get must DeepEqualsNode((NodeMaker(5, Array(), Array(2))))
      getNode(6) mustEqual None
    }

    "iterate over all nodes" in {
      graph must DeepEqualsNodeIterable((1 to 5).flatMap(getNode(_)))
    }

    "provide the correct node count" in {
      graph.nodeCount mustBe 5
    }

    "provide the correct edge count" in {
      graph.edgeCount mustBe 5L
    }

    "provide the correct max node id" in {
      graph.maxNodeId mustBe 5
    }
  }

}
