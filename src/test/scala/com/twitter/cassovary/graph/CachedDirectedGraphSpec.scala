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
    iteratorFunc, dir, "guava")

  def makeSameGraph(dir: StoredGraphDir.StoredGraphDir) = CachedDirectedGraph(
    iteratorFunc, dir, "guava", "temp-cached/sameGraph")

  def makeFastLRUGraph(dir: StoredGraphDir.StoredGraphDir) = CachedDirectedGraph(
    iteratorFunc, dir, "fastlru")

  def getNode(id: Int): Option[Node] = graph.getNodeById(id)

  val smallGraphOutOnly = beforeContext {
    graph = makeGraph(StoredGraphDir.OnlyOut)
  }

  val smallGraphInOnly = beforeContext {
    graph = makeGraph(StoredGraphDir.OnlyIn)
  }

  "Guava-based graph containing only out edges" definedAs smallGraphOutOnly should {
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

  "Guava-based graph containing only in edges" definedAs smallGraphInOnly should {
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

  "Cached graph containing only out edges" should {
    "Load a cached version successfully" in {
      var g = makeSameGraph(StoredGraphDir.OnlyOut)
      g must DeepEqualsNodeIterable((1 to 6).flatMap(g.getNodeById(_)))
      g.nodeCount mustBe 6
      g.edgeCount mustBe 10L
      var g2 = makeSameGraph(StoredGraphDir.OnlyOut)
      g2 must DeepEqualsNodeIterable((1 to 6).flatMap(g2.getNodeById(_)))
      g2.nodeCount mustBe 6
      g2.edgeCount mustBe 10L
    }
  }

  "Guava-based graph containing only out edges" should {
    var graphL: GuavaCachedDirectedGraph = null
    doBefore {
      graph = makeGraph(StoredGraphDir.OnlyOut)
      graphL = graph.asInstanceOf[GuavaCachedDirectedGraph]
    }

    "Do stats even work?" in {
      getNode(1)
      getNode(2)
      graphL.cache.stats().requestCount() mustEqual 0
      graphL.cache.stats().missCount() mustEqual 0
      getNode(1).get.neighborIds(GraphDir.OutDir)
      getNode(2).get.neighborIds(GraphDir.OutDir)
      graphL.cache.stats().requestCount() mustEqual 2
      graphL.cache.stats().missCount() mustEqual 2
    }

    "Do some values get cached?" in {
      getNode(2).get.neighborIds(GraphDir.OutDir)
      getNode(3).get.neighborIds(GraphDir.OutDir)
      getNode(2).get.neighborIds(GraphDir.OutDir)
      getNode(3).get.neighborIds(GraphDir.OutDir)
      graphL.cache.stats().requestCount() mustEqual 4
      graphL.cache.stats().missCount() mustEqual 2
    }
  }

  "FastLRU-based graph containing only out edges" should {
    var graphL: FastLRUCachedDirectedGraph = null
    doBefore {
      graph = makeFastLRUGraph(StoredGraphDir.OnlyOut)
      graphL = graph.asInstanceOf[FastLRUCachedDirectedGraph]
    }

    "map only out edges" in {
      getNode(1).get must DeepEqualsNode((NodeMaker(1, Array(2, 3, 4), Array())))
      getNode(2).get must DeepEqualsNode((NodeMaker(2, Array(1), Array())))
      getNode(3).get must DeepEqualsNode((NodeMaker(3, Array(1), Array())))
      getNode(4).get must DeepEqualsNode((NodeMaker(4, Array())))
      getNode(5).get must DeepEqualsNode((NodeMaker(5, Array(2), Array())))
      getNode(6).get must DeepEqualsNode((NodeMaker(6, Array(1, 2, 3, 4), Array())))
      getNode(7) mustEqual None
    }

    "be able to add a single node and only cache when neighborIds desired" in {
      getNode(1)
      getNode(1).get.neighborCount(GraphDir.OutDir) mustEqual 3
      graphL.cache.linkedMap.contains(1) mustEqual false
      getNode(1).get.neighborIds(GraphDir.OutDir)
      graphL.cache.linkedMap.getCurrentSize mustEqual 1
      graphL.cache.currRealCapacity mustEqual 3
      graphL.cache.linkedMap.contains(1) mustEqual true
      getNode(1).get must DeepEqualsNode((NodeMaker(1, Array(2, 3, 4), Array())))
      graphL.cache.linkedMap.contains(0) mustEqual false
      graphL.cache.linkedMap.contains(2) mustEqual false
    }

    "must evict in LRU order" in {
      getNode(2).get.neighborIds(GraphDir.OutDir)
      getNode(5).get.neighborIds(GraphDir.OutDir)
      getNode(2).get.neighborIds(GraphDir.OutDir)
      graphL.cache.linkedMap.contains(1) mustEqual false
      graphL.cache.linkedMap.contains(2) mustEqual true
      graphL.cache.linkedMap.contains(5) mustEqual true
      getNode(1).get.neighborIds(GraphDir.OutDir) // bye 5
      graphL.cache.linkedMap.contains(1) mustEqual true
      graphL.cache.linkedMap.contains(2) mustEqual true
      graphL.cache.linkedMap.contains(5) mustEqual false
    }

    "must obey index (max nodes/size of map) capacity" in {
      getNode(2).get.neighborIds(GraphDir.OutDir)
      getNode(3).get.neighborIds(GraphDir.OutDir)
      graphL.cache.linkedMap.contains(2) mustEqual true
      graphL.cache.linkedMap.contains(3) mustEqual true
      graphL.cache.linkedMap.contains(5) mustEqual false
      getNode(5).get.neighborIds(GraphDir.OutDir)
      graphL.cache.linkedMap.contains(2) mustEqual false
      graphL.cache.linkedMap.contains(3) mustEqual true
      graphL.cache.linkedMap.contains(5) mustEqual true
    }

    "must obey real (max edges) capacity" in {
      getNode(6).get.neighborIds(GraphDir.OutDir)
      getNode(6).get.neighborIds(GraphDir.OutDir)
      graphL.cache.linkedMap.contains(1) mustEqual false
      graphL.cache.linkedMap.contains(6) mustEqual true
      getNode(1).get.neighborIds(GraphDir.OutDir)
      getNode(1).get.neighborIds(GraphDir.OutDir)
      graphL.cache.linkedMap.contains(1) mustEqual true
      graphL.cache.linkedMap.contains(6) mustEqual false
    }

    "must obey both index and real capacities" in {
      getNode(2).get.neighborIds(GraphDir.OutDir)
      getNode(3).get.neighborIds(GraphDir.OutDir)
      getNode(5).get.neighborIds(GraphDir.OutDir) // bye 2, even though there's edge space
      graphL.cache.linkedMap.contains(2) mustEqual false
      graphL.cache.linkedMap.contains(3) mustEqual true
      graphL.cache.linkedMap.contains(5) mustEqual true
      graphL.cache.linkedMap.contains(6) mustEqual false
      getNode(6).get.neighborIds(GraphDir.OutDir) // bye 3, 5
      graphL.cache.linkedMap.contains(2) mustEqual false
      graphL.cache.linkedMap.contains(3) mustEqual false
      graphL.cache.linkedMap.contains(5) mustEqual false
      graphL.cache.linkedMap.contains(6) mustEqual true
    }
  }

}
