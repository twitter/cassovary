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
import com.twitter.cassovary.util.{ExecutorUtils, FastClockIntArrayCache, FastLRUIntArrayCache}
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.google.common.util.concurrent.MoreExecutors
import java.util.concurrent._
import scala.util.Random

class CachedDirectedGraphSpec extends Specification {
  var graph: CachedDirectedGraph = _

  val iteratorFunc = () => Seq(NodeIdEdgesMaxId(1, Array(2,3,4)),
    NodeIdEdgesMaxId(2, Array(1)),
    NodeIdEdgesMaxId(3, Array(1)),
    NodeIdEdgesMaxId(5, Array(2)),
    NodeIdEdgesMaxId(6, Array(1,2,3,4))).iterator

  val edgeMap = Map(1 -> Array(2,3,4), 2 -> Array(1), 3 -> Array(1),
    4 -> Array.empty, 5 -> Array(2), 6 -> Array(1,2,3,4))
  val reachability = List(0, 4, 4, 4, 1, 5, 5)

  val renumberedEdgeMap = Map(1 -> Array(2,3,6), 2 -> Array(1), 3 -> Array(1),
    6 -> Array.empty, 4 -> Array(2), 5 -> Array(1,2,3,6))
  val renumberedReachability = List(0, 4, 4, 4, 5, 5, 1)

  def makeGraph(dir: StoredGraphDir.StoredGraphDir) = CachedDirectedGraph(
    iteratorFunc, dir, "guava", renumber = false)

  def makeSameGraph(dir: StoredGraphDir.StoredGraphDir) = CachedDirectedGraph(
    iteratorFunc, dir, "guava", "temp-cached/sameGraph", false)

  def makeFastLRUGraph(dir: StoredGraphDir.StoredGraphDir) = CachedDirectedGraph(
    iteratorFunc, dir, "lru", renumber = false)

  def makeFastLRUGraphWithNodeArray(dir: StoredGraphDir.StoredGraphDir) = CachedDirectedGraph(
    iteratorFunc, dir, "lru_na", renumber = false)

  def makeFastClockGraph(dir: StoredGraphDir.StoredGraphDir) = CachedDirectedGraph(
    iteratorFunc, dir, "clock", renumber = false)

  def makeRenumberedGuavaGraph(dir: StoredGraphDir.StoredGraphDir) = CachedDirectedGraph(
    iteratorFunc, dir, "guava", renumber = true)

  def makeRenumberedFastLRUGraph(dir: StoredGraphDir.StoredGraphDir) = CachedDirectedGraph(
    iteratorFunc, dir, "lru", "temp-cached/sameGraph2", renumber = true)

  def makeRenumberedFastLRUGraphWithNodeArray(dir: StoredGraphDir.StoredGraphDir) = CachedDirectedGraph(
    iteratorFunc, dir, "lru_na", renumber = true)

  def getNode(id: Int): Option[Node] = graph.getNodeById(id)

  val smallGraphOutOnly = beforeContext {
    graph = makeGraph(StoredGraphDir.OnlyOut)
  }

  val smallGraphInOnly = beforeContext {
    graph = makeGraph(StoredGraphDir.OnlyIn)
  }

  val renumberedSmallGraphOutOnly = beforeContext {
    graph = makeRenumberedGuavaGraph(StoredGraphDir.OnlyOut)
  }

  val renumberedSmallGraphInOnly = beforeContext {
    graph = makeRenumberedGuavaGraph(StoredGraphDir.OnlyIn)
  }

  def concurrentTest(graph: CachedDirectedGraph, edgeMap: Map[Int, Array[_<:Int]], reachability: List[Int]) = {
    print("Concurrent test running...")
    val walkParams = RandomWalkParams(15000, 0.2, Some(1500), None, None, false, GraphDir.OutDir, false, true)
    val graphUtils = new GraphUtils(graph)
    // Generate a sequence of random sequences
    val r = new Random
    val x = (0 until 10).map { _ =>
      r.shuffle(Stream.continually((1 to 6).toList).flatten.take(10).toList)
    }.toSeq
    // Launch many threads each doing personalized reputation
    val futures = ExecutorUtils.parallelWork[List[Int], List[Int]](Executors.newFixedThreadPool(10),
    x,
    { intList =>
      intList.map { i =>
        val (a, b) = graphUtils.debugCalculatePersonalizedReputation(i, walkParams, edgeMap)
        //println(i, a)
        a.size
      }
    })
    // Wait for all threads to complete
    val intLists = futures.toArray.map { f => f.asInstanceOf[Future[List[Int]]].get }
    // Test reachability for all of them
    (0 until 10).foreach { i =>
      (0 until 10).foreach { j =>
        intLists(i)(j) mustEqual reachability(x(i)(j))
      }
    }
    print("done!\n")
  }

  "Renumbered FastLRU-based graph containing only out edges" should {
    var graphL: FastCachedDirectedGraph = null
    var graphLCache: FastLRUIntArrayCache = null
    doBefore {
      graph = makeRenumberedFastLRUGraph(StoredGraphDir.OnlyOut)
      graph = makeRenumberedFastLRUGraph(StoredGraphDir.OnlyOut)
      graphL = graph.asInstanceOf[FastCachedDirectedGraph]
      graphLCache = graphL.cache.asInstanceOf[FastLRUIntArrayCache]
    }

    "map only out edges" in {
      getNode(1).get must DeepEqualsNode((NodeMaker(1, Array(2, 3, 6), Array())))
      getNode(2).get must DeepEqualsNode((NodeMaker(2, Array(1), Array())))
      getNode(3).get must DeepEqualsNode((NodeMaker(3, Array(1), Array())))
      getNode(4).get must DeepEqualsNode((NodeMaker(4, Array(2), Array())))
      getNode(5).get must DeepEqualsNode((NodeMaker(5, Array(1, 2, 3, 6), Array())))
      getNode(6).get must DeepEqualsNode((NodeMaker(6, Array())))
      getNode(7) mustEqual None
      getNode(100) mustEqual None
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

    "Do a concurrent random walk properly" in {
      concurrentTest(graphL, renumberedEdgeMap, renumberedReachability)
    }
  }

  "Renumbered Node Array-based FastLRU-based graph containing only out edges" should {
    var graphL: NodeArrayFastCachedDirectedGraph = null
    var graphLCache: FastLRUIntArrayCache = null
    doBefore {
      graph = makeRenumberedFastLRUGraphWithNodeArray(StoredGraphDir.OnlyOut)
      graphL = graph.asInstanceOf[NodeArrayFastCachedDirectedGraph]
      graphLCache = graphL.cache.asInstanceOf[FastLRUIntArrayCache]
    }

    "map only out edges" in {
      getNode(1).get must DeepEqualsNode((NodeMaker(1, Array(2, 3, 6), Array())))
      getNode(2).get must DeepEqualsNode((NodeMaker(2, Array(1), Array())))
      getNode(3).get must DeepEqualsNode((NodeMaker(3, Array(1), Array())))
      getNode(4).get must DeepEqualsNode((NodeMaker(4, Array(2), Array())))
      getNode(5).get must DeepEqualsNode((NodeMaker(5, Array(1, 2, 3, 6), Array())))
      getNode(6).get must DeepEqualsNode((NodeMaker(6, Array())))
      getNode(7) mustEqual None
      getNode(100) mustEqual None
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

  "Renumbered guava-based graph containing only out edges" definedAs renumberedSmallGraphOutOnly should {
    "map only out edges" in {
      getNode(1).get must DeepEqualsNode((NodeMaker(1, Array(2, 3, 6), Array())))
      getNode(2).get must DeepEqualsNode((NodeMaker(2, Array(1), Array())))
      getNode(3).get must DeepEqualsNode((NodeMaker(3, Array(1), Array())))
      getNode(4).get must DeepEqualsNode((NodeMaker(4, Array(2), Array())))
      getNode(5).get must DeepEqualsNode((NodeMaker(5, Array(1, 2, 3, 6), Array())))
      getNode(6).get must DeepEqualsNode((NodeMaker(6, Array())))
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

  "Renumbered guava-based graph containing only in edges" definedAs renumberedSmallGraphInOnly should {
    "map only in edges" in {
      getNode(1).get must DeepEqualsNode((NodeMaker(1, Array(), Array(2, 3, 6))))
      getNode(2).get must DeepEqualsNode((NodeMaker(2, Array(), Array(1))))
      getNode(3).get must DeepEqualsNode((NodeMaker(3, Array(), Array(1))))
      getNode(4).get must DeepEqualsNode((NodeMaker(4, Array(), Array(2))))
      getNode(5).get must DeepEqualsNode((NodeMaker(5, Array(), Array(1, 2, 3, 6))))
      getNode(6).get must DeepEqualsNode((NodeMaker(6, Array())))
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
      graphL.cacheG.stats().requestCount() mustEqual 0
      graphL.cacheG.stats().missCount() mustEqual 0
      getNode(1).get.neighborIds(GraphDir.OutDir)
      getNode(2).get.neighborIds(GraphDir.OutDir)
      graphL.cacheG.stats().requestCount() mustEqual 2
      graphL.cacheG.stats().missCount() mustEqual 2
    }

    "Do some values get cached?" in {
      getNode(2).get.neighborIds(GraphDir.OutDir)
      getNode(3).get.neighborIds(GraphDir.OutDir)
      getNode(2).get.neighborIds(GraphDir.OutDir)
      getNode(3).get.neighborIds(GraphDir.OutDir)
      graphL.cacheG.stats().requestCount() mustEqual 4
      graphL.cacheG.stats().missCount() mustEqual 2
    }
  }

  "FastClock-based graph containing only out edges" should {
    var graphC: FastCachedDirectedGraph = null
    var graphCCache: FastClockIntArrayCache = null
    doBefore {
      graph = makeFastClockGraph(StoredGraphDir.OnlyOut)
      graphC = graph.asInstanceOf[FastCachedDirectedGraph]
      graphCCache = graphC.cache.asInstanceOf[FastClockIntArrayCache]
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
      graphCCache.contains(1) mustEqual false
      getNode(1).get.neighborIds(GraphDir.OutDir)
      graphCCache.currNodeCapacity mustEqual 1
      graphCCache.currRealCapacity mustEqual 3
      graphCCache.contains(1) mustEqual true
      getNode(1).get must DeepEqualsNode((NodeMaker(1, Array(2, 3, 4), Array())))
      graphCCache.contains(0) mustEqual false
      graphCCache.contains(2) mustEqual false
    }

    "Do a concurrent random walk properly" in {
      concurrentTest(graph, edgeMap, reachability)
    }
  }

  "FastLRU-based graph containing only out edges" should {
    var graphL: FastCachedDirectedGraph = null
    var graphLCache: FastLRUIntArrayCache = null
    doBefore {
      graph = makeFastLRUGraph(StoredGraphDir.OnlyOut)
      graphL = graph.asInstanceOf[FastCachedDirectedGraph]
      graphLCache = graphL.cache.asInstanceOf[FastLRUIntArrayCache]
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
      graphLCache.linkedMap.contains(1) mustEqual false
      getNode(1).get.neighborIds(GraphDir.OutDir)
      graphLCache.linkedMap.getCurrentSize mustEqual 1
      graphLCache.currRealCapacity mustEqual 3
      graphLCache.linkedMap.contains(1) mustEqual true
      getNode(1).get must DeepEqualsNode((NodeMaker(1, Array(2, 3, 4), Array())))
      graphLCache.linkedMap.contains(0) mustEqual false
      graphLCache.linkedMap.contains(2) mustEqual false
    }

    "must evict in LRU order" in {
      getNode(2).get.neighborIds(GraphDir.OutDir)
      getNode(5).get.neighborIds(GraphDir.OutDir)
      getNode(2).get.neighborIds(GraphDir.OutDir)
      graphLCache.linkedMap.contains(1) mustEqual false
      graphLCache.linkedMap.contains(2) mustEqual true
      graphLCache.linkedMap.contains(5) mustEqual true
      getNode(1).get.neighborIds(GraphDir.OutDir) // bye 5
      graphLCache.linkedMap.contains(1) mustEqual true
      graphLCache.linkedMap.contains(2) mustEqual true
      graphLCache.linkedMap.contains(5) mustEqual false
    }

    "must obey index (max nodes/size of map) capacity" in {
      getNode(2).get.neighborIds(GraphDir.OutDir)
      getNode(3).get.neighborIds(GraphDir.OutDir)
      graphLCache.linkedMap.contains(2) mustEqual true
      graphLCache.linkedMap.contains(3) mustEqual true
      graphLCache.linkedMap.contains(5) mustEqual false
      getNode(5).get.neighborIds(GraphDir.OutDir)
      graphLCache.linkedMap.contains(2) mustEqual false
      graphLCache.linkedMap.contains(3) mustEqual true
      graphLCache.linkedMap.contains(5) mustEqual true
    }

    "must obey real (max edges) capacity" in {
      getNode(6).get.neighborIds(GraphDir.OutDir)
      getNode(6).get.neighborIds(GraphDir.OutDir)
      graphLCache.linkedMap.contains(1) mustEqual false
      graphLCache.linkedMap.contains(6) mustEqual true
      getNode(1).get.neighborIds(GraphDir.OutDir)
      getNode(1).get.neighborIds(GraphDir.OutDir)
      graphLCache.linkedMap.contains(1) mustEqual true
      graphLCache.linkedMap.contains(6) mustEqual false
    }

    "must obey both index and real capacities" in {
      getNode(2).get.neighborIds(GraphDir.OutDir)
      getNode(3).get.neighborIds(GraphDir.OutDir)
      getNode(5).get.neighborIds(GraphDir.OutDir) // bye 2, even though there's edge space
      graphLCache.linkedMap.contains(2) mustEqual false
      graphLCache.linkedMap.contains(3) mustEqual true
      graphLCache.linkedMap.contains(5) mustEqual true
      graphLCache.linkedMap.contains(6) mustEqual false
      getNode(6).get.neighborIds(GraphDir.OutDir) // bye 3, 5
      graphLCache.linkedMap.contains(2) mustEqual false
      graphLCache.linkedMap.contains(3) mustEqual false
      graphLCache.linkedMap.contains(5) mustEqual false
      graphLCache.linkedMap.contains(6) mustEqual true
    }

    "Do a random walk properly" in {
      val walkParams = RandomWalkParams(15000, 0.2, Some(1500), None, None, false, GraphDir.OutDir, false, true)
      val graphUtils = new GraphUtils(graphL)
      val (a, b) = graphUtils.calculatePersonalizedReputation(2, walkParams)
      a.size() mustEqual 4
    }

    "Do a concurrent random walk properly" in {
      concurrentTest(graphL, edgeMap, reachability)
    }
  }

  "Node Array FastLRU-based graph containing only out edges" should {
    "Do a concurrent random walk properly" in {
      graph = makeFastLRUGraphWithNodeArray(StoredGraphDir.OnlyOut)
      concurrentTest(graph, edgeMap, reachability)
    }
  }

}
