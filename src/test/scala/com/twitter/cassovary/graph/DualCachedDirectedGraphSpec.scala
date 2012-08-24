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
import com.google.common.util.concurrent.MoreExecutors

class DualCachedDirectedGraphSpec extends Specification {
  var graph: CachedDirectedGraph = _

  val iteratorFunc = () => Seq(NodeIdEdgesMaxId(1, Array(2,3,4)),
    NodeIdEdgesMaxId(2, Array(1)),
    NodeIdEdgesMaxId(3, Array(1)),
    NodeIdEdgesMaxId(5, Array(2)),
    NodeIdEdgesMaxId(6, Array(1,2,3,4))).iterator

  val inIteratorFunc = () => Seq(NodeIdEdgesMaxId(1, Array(2,3,6)),
    NodeIdEdgesMaxId(2, Array(1, 5, 6)),
    NodeIdEdgesMaxId(3, Array(1, 6)),
    NodeIdEdgesMaxId(4, Array(1, 6))).iterator

  def getNode(id: Int): Option[Node] = graph.getNodeById(id)

  def makeRenumberedDualGraph =
    CachedDirectedGraph(Seq(iteratorFunc), Seq(inIteratorFunc), MoreExecutors.sameThreadExecutor(),
      StoredGraphDir.BothInOut, "lru", 2, 4, Array("temp-shards/d"), 2, 2, true, "temp-cached/dtwoshards", true)

  def makeDualGraph =
    CachedDirectedGraph(Seq(iteratorFunc), Seq(inIteratorFunc), MoreExecutors.sameThreadExecutor(),
      StoredGraphDir.BothInOut, "lru", 2, 4, Array("temp-shards/d"), 2, 2, true, "temp-cached/dtwoshards", false)

  "Regular FastLRU-based Dual Graph containing only out edges" should {
    doBefore {
      graph = makeDualGraph
    }

    "map only out edges" in {
      getNode(1).get must DeepEqualsNode((NodeMaker(1, Array(2, 3, 4), Array(2, 3, 6))))
      getNode(2).get must DeepEqualsNode((NodeMaker(2, Array(1), Array(1, 5, 6))))
      getNode(3).get must DeepEqualsNode((NodeMaker(3, Array(1), Array(1, 6))))
      getNode(4).get must DeepEqualsNode((NodeMaker(4, Array(), Array(1, 6))))
      getNode(5).get must DeepEqualsNode((NodeMaker(5, Array(2), Array())))
      getNode(6).get must DeepEqualsNode((NodeMaker(6, Array(1, 2, 3, 4), Array())))
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

  "Renumbered FastLRU-based Dual Graph containing only out edges" should {
    doBefore {
      graph = makeRenumberedDualGraph
    }

    "map only out edges" in {
      getNode(1).get must DeepEqualsNode((NodeMaker(1, Array(2, 3, 6), Array(2, 3, 5))))
      getNode(2).get must DeepEqualsNode((NodeMaker(2, Array(1), Array(1, 4, 5))))
      getNode(3).get must DeepEqualsNode((NodeMaker(3, Array(1), Array(1, 5))))
      getNode(4).get must DeepEqualsNode((NodeMaker(4, Array(2), Array())))
      getNode(5).get must DeepEqualsNode((NodeMaker(5, Array(1, 2, 3, 6), Array())))
      getNode(6).get must DeepEqualsNode((NodeMaker(6, Array(), Array(1, 5))))
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

}
