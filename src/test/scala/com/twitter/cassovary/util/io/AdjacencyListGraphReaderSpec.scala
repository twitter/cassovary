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
package com.twitter.cassovary.util.io

import com.twitter.cassovary.graph.DirectedGraph
import com.twitter.cassovary.graph.LabeledNode
import com.twitter.cassovary.graph.{NodeRenumberer,SequentialNodeRenumberer}
import java.util.concurrent.Executors
import com.google.common.util.concurrent.MoreExecutors
import org.specs.Specification

class AdjacencyListGraphReaderSpec extends Specification  {

  val nodeMap = Map( 10 -> List(11, 12, 13), 11 -> List(12, 14), 12 -> List(14),
    13 -> List(12, 14), 14 -> List(15), 15 -> List(10, 11))

  val labelMap = Map( 10 -> 0, 11 -> 1, 12 -> -1, 13 -> -1, 14 -> 0, 15 -> -1)

  /**
   * Compares the nodes in a graph and those defined by the nodeMap (id -> ids of neighbors),
   * and ensures that these are equivalent
   * @param g DirectedGraph
   * @param nodeMap Map of node ids to ids of its neighbors
   */
  def nodeMapEquals(g:DirectedGraph, nodeMap: Map[Int, List[Int]]) = {
    g.foreach { node =>
      nodeMap.contains(node.id) mustBe true
      val neighbors = node.outboundNodes()
      val nodesInMap = nodeMap(node.id)
      nodesInMap.foreach { i => neighbors.contains(i) mustBe true }
      neighbors.foreach { i => nodesInMap.contains(i) mustBe true }
    }
    nodeMap.keys.foreach { id => g.existsNodeId(id) mustBe true }
  }

  /**
   * Compares the nodes in a graph and those defined by the nodeMap (id -> ids of neighbors),
   * remapping node ids thru nodeRenumberer, and ensures that these are equivalent
   * @param g DirectedGraph
   * @param nodeMap Map of node ids to ids of its neighbors
   * @param nodeRenumberer a node renumberer
   */
  def nodeMapEqualsRenumbered(g:DirectedGraph, nodeMap: Map[Int, List[Int]], nodeRenumberer: NodeRenumberer) = {
    g.foreach { node =>
      nodeMap.contains(nodeRenumberer.nodeIdxToNodeId(node.id)) mustBe true
      val neighbors = node.outboundNodes()
      val nodesInMap = nodeMap(nodeRenumberer.nodeIdxToNodeId(node.id))
      nodesInMap.foreach { i => neighbors.contains(nodeRenumberer.nodeIdToNodeIdx(i)) mustBe true }
      neighbors.foreach { i => nodesInMap.contains(nodeRenumberer.nodeIdxToNodeId(i)) mustBe true }
    }
    nodeMap.keys.foreach { id => g.existsNodeId(nodeRenumberer.nodeIdToNodeIdx(id)) mustBe true }
  }

  /**
   * Compares the labels of nodes in a graph to those defined by a label map.
   */
  def labelListEquals(g: DirectedGraph, labelMap: Map[Int,Int]) = {
    g.foreach { node =>
      labelMap.contains(node.id) mustBe true
      node.asInstanceOf[LabeledNode].label mustBe labelMap(node.id)
    }
  }

  /**
   * Compares the labels of nodes in a graph to those defined by a label map.
   */
  def labelListEqualsRenumbered(g: DirectedGraph, labelMap: Map[Int,Int], nodeRenumberer: NodeRenumberer) = {
    g.foreach { node =>
      labelMap.contains(nodeRenumberer.nodeIdxToNodeId(node.id)) mustBe true
      node.asInstanceOf[LabeledNode].label mustBe labelMap(nodeRenumberer.nodeIdxToNodeId(node.id))
    }
  }

  var graph: DirectedGraph = _

  "AdjacencyListReader" should {

    doBefore{
      // Example using 2 threads to read in the graph
      graph = new AdjacencyListGraphReader("src/test/resources/graphs/", "toy_6nodes_adj",
                                           new LabeledVertexReaderFactory()) {
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

  "AdjacencyListReader serial labeled" should {

    doBefore{
      graph = new AdjacencyListGraphReader("src/test/resources/graphs/", "toy_6nodes-2_labeled", new LabeledVertexReaderFactory()) {
          override val executorService = MoreExecutors.sameThreadExecutor()
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

    "contain the right labels" in {
      labelListEquals(graph, labelMap)
    }

  }

  "AdjacencyListReader parallel labeled" should {

    doBefore{
      graph = new AdjacencyListGraphReader("src/test/resources/graphs/", "toy_6nodes_labeled_adj", new LabeledVertexReaderFactory()) {
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

    "contain the right labels" in {
      labelListEquals(graph, labelMap)
    }

  }

  "AdjacencyListReader serial labeled renumbered" should {

    var seqRenumberer = new SequentialNodeRenumberer()

    doBefore{
      graph = new AdjacencyListGraphReader("src/test/resources/graphs/", "toy_6nodes-2_labeled", new LabeledVertexReaderFactory(), seqRenumberer) {
          override val executorService = MoreExecutors.sameThreadExecutor()
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

    "contain the right renumbered labels" in {
      labelListEqualsRenumbered(graph, labelMap, seqRenumberer)
    }

  }

  "AdjacencyListReader parallel labeled renumbered" should {

    var seqRenumberer = new SequentialNodeRenumberer()

    doBefore{
      graph = new AdjacencyListGraphReader("src/test/resources/graphs/", "toy_6nodes_labeled_adj", new LabeledVertexReaderFactory(), seqRenumberer) {
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

    "contain the right renumbered labels" in {
      labelListEqualsRenumbered(graph, labelMap, seqRenumberer)
    }

  }


}
