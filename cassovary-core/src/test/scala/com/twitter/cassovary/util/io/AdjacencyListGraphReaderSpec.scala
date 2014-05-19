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
package com.twitter.cassovary.util.io

import com.twitter.cassovary.graph.DirectedGraph
import com.twitter.cassovary.util.{NodeNumberer,SequentialNodeNumberer}
import java.util.concurrent.Executors
import org.specs.Specification

class AdjacencyListGraphReaderSpec extends Specification with GraphMapEquality {

  val nodeMap = Map( 10 -> List(11, 12, 13), 11 -> List(12, 14), 12 -> List(14),
    13 -> List(12, 14), 14 -> List(15), 15 -> List(10, 11))

  /**
   * Compares the nodes in a graph and those defined by the nodeMap (id -> ids of neighbors),
   * remapping node ids thru nodeRenumberer, and ensures that these are equivalent
   * @param g DirectedGraph
   * @param nodeMap Map of node ids to ids of its neighbors
   * @param nodeNumberer a node renumberer
   */
  def nodeMapEqualsRenumbered(g:DirectedGraph, nodeMap: Map[Int, List[Int]], nodeNumberer: NodeNumberer[Int]) = {
    g.foreach { node =>
      nodeMap.contains(nodeNumberer.internalToExternal(node.id)) mustBe true
      val neighbors = node.outboundNodes()
      val nodesInMap = nodeMap(nodeNumberer.internalToExternal(node.id))
      nodesInMap.foreach { i => neighbors.contains(nodeNumberer.externalToInternal(i)) mustBe true }
      neighbors.foreach { i => nodesInMap.contains(nodeNumberer.internalToExternal(i)) mustBe true }
    }
    nodeMap.keys.foreach { id => g.existsNodeId(nodeNumberer.externalToInternal(id)) mustBe true }
  }

  var graph: DirectedGraph = _

  "AdjacencyListReader" should {

    doBefore{
      // Example using 2 threads to read in the graph
      graph = new AdjacencyListGraphReader("cassovary-core/src/test/resources/graphs/", "toy_6nodes_adj") {
          override val executorService = Executors.newFixedThreadPool(2)
        }.toSharedArrayBasedDirectedGraph()
    }

    "provide the correct graph properties" in {
      graph.nodeCount mustBe 6
      graph.edgeCount mustBe 11L
      graph.maxNodeId mustBe 15
    }

    "contain the right nodes and edges" in {
      nodeMapEquals(graph, nodeMap)
    }

  }

  "AdjacencyListReader renumbered" should {
    var seqRenumberer = new SequentialNodeNumberer[Int]()

    doBefore{
      graph = new AdjacencyListGraphReader("cassovary-core/src/test/resources/graphs/", "toy_6nodes_adj",
        seqRenumberer) {
          override val executorService = Executors.newFixedThreadPool(2)
        }.toSharedArrayBasedDirectedGraph()
    }

    "provide the correct graph properties" in {
      graph.nodeCount mustBe 6
      graph.edgeCount mustBe 11L
      graph.maxNodeId mustBe 5  // Zero-based; node ids are 0 thru 5.
    }

    "contain the right renumbered nodes and edges" in {
      nodeMapEqualsRenumbered(graph, nodeMap, seqRenumberer)
    }
  }

}
