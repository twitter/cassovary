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

import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.util.{Sampling, BinomialDistribution}
import it.unimi.dsi.fastutil.ints.{IntOpenHashSet, IntArrayList}
import scala.collection._
import scala.collection.JavaConversions._
import scala.util.Random

/**
 * A simple implementation of a DirectedGraph
 */
case class TestGraph(nodes: Node*) extends DirectedGraph {
  val nodeTable = new mutable.HashMap[Int, Node]
  nodes foreach { addNode }

  def nodeCount = nodeTable.size
  def iterator = nodeTable.valuesIterator

  def edgeCount = iterator.foldLeft(0L) {
    (accum, node) => accum + node.outboundCount
  }
  def getNodeById(id: Int) = nodeTable.get(id)

  def addNode(node: Node) {
    nodeTable.put(node.id, node)
  }

  def removeNode(node: Node) {
    nodeTable.remove(node.id)
  }

  override val storedGraphDir = StoredGraphDir.BothInOut
}

/**
 * Some sample graphs for quick testing in tests.
 */
object TestGraphs {
  val g1 = TestGraph(TestNode(1, Nil, Nil))

  val g2_mutual = TestGraph(
    TestNode(1, List(2), List(2)),
    TestNode(2, List(1), List(1))
    )

  val g2_nonmutual = TestGraph(
    TestNode(1, List(2), Nil),
    TestNode(2, Nil, List(1))
    )

  def g3 = ArrayBasedDirectedGraph( () => Seq(
    NodeIdEdgesMaxId(10, Array(11, 12)),
    NodeIdEdgesMaxId(11, Array(12)),
    NodeIdEdgesMaxId(12, Array(11))
    ).iterator, StoredGraphDir.BothInOut)

  val nodeSeqIterator = () => Seq(
      NodeIdEdgesMaxId(10, Array(11, 12, 13)),
      NodeIdEdgesMaxId(11, Array(12, 14)),
      NodeIdEdgesMaxId(12, Array(14)),
      NodeIdEdgesMaxId(13, Array(12, 14)),
      NodeIdEdgesMaxId(14, Array(15)),
      NodeIdEdgesMaxId(15, Array(10, 11))
      ).iterator

  // using testGraph becomes onerous for non-trivial graphs
  def g6 = ArrayBasedDirectedGraph(nodeSeqIterator, StoredGraphDir.BothInOut)

  def g6_onlyout = ArrayBasedDirectedGraph(nodeSeqIterator, StoredGraphDir.OnlyOut)
  def g6_onlyin = ArrayBasedDirectedGraph(nodeSeqIterator, StoredGraphDir.OnlyIn)

  val nodeSeqIterator2 = () => Seq(
      NodeIdEdgesMaxId(10, Array(11, 12, 13)),
      NodeIdEdgesMaxId(11, Array(10, 13, 14)),
      NodeIdEdgesMaxId(12, Array(13, 14)),
      NodeIdEdgesMaxId(13, Array(12, 14)),
      NodeIdEdgesMaxId(14, Array(10, 11, 15)),
      NodeIdEdgesMaxId(15, Array(10, 11, 16)),
      NodeIdEdgesMaxId(16, Array(15))
      ).iterator
  def g7_onlyout = ArrayBasedDirectedGraph(nodeSeqIterator2, StoredGraphDir.OnlyOut)
  def g7_onlyin = ArrayBasedDirectedGraph(nodeSeqIterator2, StoredGraphDir.OnlyIn)

  // a complete graph is where each node follows every other node
  def generateCompleteGraph(numNodes: Int) = {
    val allNodes = (1 to numNodes).toList
    val testNodes = (1 to numNodes).toList map { source =>
      val allBut = allNodes.filter(_ != source)
      TestNode(source, allBut, allBut)
    }
    TestGraph(testNodes: _*)
  }



  /**
   * @param numNodes number of nodes in the undirected graph
   * @param avgOutDegree average out degree of each node
   * @param graphDir store both directions or only one direction
   * @return a random Erdos-Renyi Directed graph
   */
  def generateRandomGraph(numNodes: Int, avgOutDegree: Int, graphDir: StoredGraphDir = StoredGraphDir.BothInOut) = {
    val nodes = new Array[NodeIdEdgesMaxId](numNodes)
    val rand = new Random
    val probEdge = avgOutDegree.toDouble / (numNodes - 1)
    val nodesOutDegrees = new BinomialDistribution(numNodes - 1, probEdge).sample(rand).iterator
    (0 until numNodes) foreach { source =>
      val outDegree = nodesOutDegrees.next()
      val outNeighbors = Sampling.randomSubset(outDegree, ((0 until source) ++ (source + 1 to numNodes - 1)).toArray,
        rand)
      nodes(source) = NodeIdEdgesMaxId(source, outNeighbors.toArray)
    }
    ArrayBasedDirectedGraph( () => nodes.iterator, graphDir)
  }

  /**
   * @param numNodes number of nodes in the undirected graph
   * @param probEdge probability of existence of an edge, constant for each of (numNodes choose 2) edges
   * @param graphDir store both directions or only one direction
   * @return a random Erdos-Renyi undirected graph (a graph which only has mutual edges)
   */
  def generateRandomUndirectedGraph(numNodes: Int, probEdge: Double,
                                    graphDir: StoredGraphDir = StoredGraphDir.BothInOut) = {
    val nodes = new Array[IntArrayList](numNodes) map { _ => new IntArrayList() }
    val rand = new Random
    val nodesOutDegrees = new BinomialDistribution(numNodes - 1, probEdge).sample(rand).iterator
    (0 until numNodes - 1) foreach {
      source =>
        val newNeighborsNumber = nodesOutDegrees.next() - nodes(source).length
        if (newNeighborsNumber > 0) {
          val newNeighbors = Sampling.randomSubset(newNeighborsNumber,
            (source + 1 to numNodes - 1).toArray, rand)
          newNeighbors.foreach {
            n =>
              nodes(source).add(n)
              nodes(n).add(source)
          }
        }
    }
    val nodesEdges = nodes.indices map { i =>
      val arr = new Array[Int](nodes(i).size)
      NodeIdEdgesMaxId(i, nodes(i).toIntArray(arr) )
    }
    ArrayBasedDirectedGraph( () => nodesEdges.iterator, graphDir)
  }

}
