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
  var graph: CachedDirectedGraph = _

  val iteratorFunc = () => Seq(NodeIdEdgesMaxId(1, Array(2,3,4)),
    NodeIdEdgesMaxId(2, Array(1)),
    NodeIdEdgesMaxId(3, Array(1)),
    NodeIdEdgesMaxId(5, Array(2)),
    NodeIdEdgesMaxId(6, Array(1,2,3,4))).iterator

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
      getNode(3).get must DeepEqualsNode((NodeMaker(3, Array(1), Array())))
      getNode(4).get must DeepEqualsNode((NodeMaker(4, Array())))
      getNode(5).get must DeepEqualsNode((NodeMaker(5, Array(2), Array())))
      getNode(6).get must DeepEqualsNode((NodeMaker(6, Array(1, 2, 3, 4), Array())))
      getNode(7) mustEqual None
    }

    "iterate over all nodes" in {
      graph must DeepEqualsNodeIterable((1 to 6).flatMap(getNode(_)))
    }

    "provide the correct node count" in {
      graph.nodeCount mustBe 6
    }

    "provide the correct edge count" in {
      graph.edgeCount mustBe 10L
    }
  }

  "graph containing only in edges" definedAs smallGraphInOnly should {
    "map only in edges" in {
      getNode(1).get must DeepEqualsNode((NodeMaker(1, Array(), Array(2, 3, 4))))
      getNode(2).get must DeepEqualsNode((NodeMaker(2, Array(), Array(1))))
      getNode(3).get must DeepEqualsNode((NodeMaker(3, Array(), Array(1))))
      getNode(4).get must DeepEqualsNode((NodeMaker(4, Array())))
      getNode(5).get must DeepEqualsNode((NodeMaker(5, Array(), Array(2))))
      getNode(6).get must DeepEqualsNode((NodeMaker(6, Array(), Array(1, 2, 3, 4))))
      getNode(7) mustEqual None
    }

    "iterate over all nodes" in {
      graph must DeepEqualsNodeIterable((1 to 6).flatMap(getNode(_)))
    }

    "provide the correct node count" in {
      graph.nodeCount mustBe 6
    }

    "provide the correct edge count" in {
      graph.edgeCount mustBe 10L
    }

    "provide the correct max node id" in {
      graph.maxNodeId mustBe 6
    }
  }

  "graph containing only out edges" should {
    doBefore {
      graph = makeGraph(StoredGraphDir.OnlyOut)
    }

    "be able to add a single node" in {
      getNode(1)
      graph.cache.getCurrentSize mustEqual 1
      graph.currRealCapacity mustEqual 3
      graph.cache.contains(1) mustEqual true
      graph.indexToObject(graph.cache.getIndexFromId(1)) must DeepEqualsNode((NodeMaker(1, Array(2, 3, 4), Array())))
      graph.cache.contains(0) mustEqual false
      graph.cache.contains(2) mustEqual false
    }

    "must evict in LRU order" in {
      getNode(2)
      getNode(5)
      getNode(2)
      graph.cache.contains(1) mustEqual false
      graph.cache.contains(2) mustEqual true
      graph.cache.contains(5) mustEqual true
      getNode(1) // bye 5
      graph.cache.contains(1) mustEqual true
      graph.cache.contains(2) mustEqual true
      graph.cache.contains(5) mustEqual false
    }

    "must obey index (max nodes/size of map) capacity" in {
      getNode(2)
      getNode(3)
      graph.cache.contains(2) mustEqual true
      graph.cache.contains(3) mustEqual true
      graph.cache.contains(5) mustEqual false
      getNode(5)
      graph.cache.contains(2) mustEqual false
      graph.cache.contains(3) mustEqual true
      graph.cache.contains(5) mustEqual true
    }

    "must obey real (max edges) capacity" in {
      getNode(6)
      getNode(6)
      graph.cache.contains(1) mustEqual false
      graph.cache.contains(6) mustEqual true
      getNode(1)
      getNode(1)
      graph.cache.contains(1) mustEqual true
      graph.cache.contains(6) mustEqual false
    }

    "must obey both index and real capacities" in {
      getNode(2)
      getNode(3)
      getNode(5) // bye 2, even though there's edge space
      graph.cache.contains(2) mustEqual false
      graph.cache.contains(3) mustEqual true
      graph.cache.contains(5) mustEqual true
      graph.cache.contains(6) mustEqual false
      getNode(6) // bye 3, 5
      graph.cache.contains(2) mustEqual false
      graph.cache.contains(3) mustEqual false
      graph.cache.contains(5) mustEqual false
      graph.cache.contains(6) mustEqual true
    }

//    "must update the order of the list and obey real capacity" in {
//      cache.addToHead(14, n14)
//      cache.addToHead(13, n13)
//      cache.addToHead(12, n12)
//      cache.addToHead(10, n10)
//      cache.moveToHead(14)
//      cache.addToHead(15, n15) // bye 13
//      cache.contains(10) mustEqual true
//      cache.contains(11) mustEqual false
//      cache.contains(12) mustEqual true
//      cache.contains(13) mustEqual false
//      cache.contains(14) mustEqual true
//      cache.contains(15) mustEqual true
//      cache.moveToHead(10)
//      cache.addToHead(13, n13) // bye 12
//      cache.contains(10) mustEqual true
//      cache.contains(11) mustEqual false
//      cache.contains(12) mustEqual false
//      cache.contains(13) mustEqual true
//      cache.contains(14) mustEqual true
//      cache.contains(15) mustEqual true
//      cache.map.getCurrentSize mustEqual 4
//      cache.currRealCapacity mustEqual 8
//      cache.addToHead(11, n11) // bye 14, 15
//      cache.contains(10) mustEqual true
//      cache.contains(11) mustEqual true
//      cache.contains(12) mustEqual false
//      cache.contains(13) mustEqual true
//      cache.contains(14) mustEqual false
//      cache.contains(15) mustEqual false
//      cache.map.getCurrentSize mustEqual 3
//      cache.currRealCapacity mustEqual 7
//    }
  }

}
