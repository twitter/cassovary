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

import scala.collection.mutable
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


  // using testGraph becomes onerous for non-trivial graphs
  def g6 = ArrayBasedDirectedGraph( () => Seq(
    NodeIdEdgesMaxId(10, Array(11, 12, 13)),
    NodeIdEdgesMaxId(11, Array(12, 14)),
    NodeIdEdgesMaxId(12, Array(14)),
    NodeIdEdgesMaxId(13, Array(12, 14)),
    NodeIdEdgesMaxId(14, Array(15)),
    NodeIdEdgesMaxId(15, Array(10, 11))
    ).iterator, StoredGraphDir.BothInOut)

  // a complete graph is where each node follows every other node
  def generateCompleteGraph(numNodes: Int) = {
    val allNodes = (1 to numNodes).toList
    val testNodes = (1 to numNodes).toList map { source =>
      val allBut = allNodes.filter(_ != source)
      TestNode(source, allBut, allBut)
    }
    TestGraph(testNodes: _*)
  }

  def generateRandomGraph(numNodes: Int, avgOutDegree: Int) = {
    val nodes = new mutable.ArrayBuffer[NodeIdEdgesMaxId]
    val rand = new Random
    (0 until numNodes) foreach { source =>
      val numOutNeighbors = rand.nextInt(2 * avgOutDegree + 1)
      val outNeighbors = (0 until numOutNeighbors).toArray.map { i: Int => rand.nextInt(numNodes) }
      nodes += NodeIdEdgesMaxId(source, outNeighbors)
    }
    ArrayBasedDirectedGraph( () => nodes.iterator, StoredGraphDir.BothInOut)
  }
}
