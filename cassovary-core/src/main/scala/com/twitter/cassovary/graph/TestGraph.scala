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

import com.twitter.cassovary.graph.bipartite._
import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.util.{BinomialDistribution, BoundedFuturePool, Sampling}
import com.twitter.util.{Await, Future, FuturePool}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

/**
 * A simple implementation of a DirectedGraph
 */
case class TestGraph(nodes: Node*) extends DirectedGraph[Node] {
  val nodeTable = new mutable.HashMap[Int, Node]
  nodes foreach { addNode }

  def nodeCount = nodeTable.size
  def iterator = nodeTable.valuesIterator

  def edgeCount = iterator.foldLeft(0L) {
    (accum, node) => accum + node.outboundCount + node.inboundCount
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

  val g3_dangling_seq = Seq(
    NodeIdEdgesMaxId(0, Array(1)),
    NodeIdEdgesMaxId(1, Array(2)),
    NodeIdEdgesMaxId(2, Array()))

  val g3_dangling_out = ArrayBasedDirectedGraph(g3_dangling_seq, StoredGraphDir.OnlyOut,
    NeighborsSortingStrategy.LeaveUnsorted)

  val g3_dangling_in = ArrayBasedDirectedGraph(g3_dangling_seq, StoredGraphDir.OnlyIn,
    NeighborsSortingStrategy.LeaveUnsorted)

  def g3 = ArrayBasedDirectedGraph(Seq(
    NodeIdEdgesMaxId(10, Array(11, 12)),
    NodeIdEdgesMaxId(11, Array(12)),
    NodeIdEdgesMaxId(12, Array(11))
    ), StoredGraphDir.BothInOut, NeighborsSortingStrategy.LeaveUnsorted)

  def g5 = ArrayBasedDirectedGraph(Seq(
    NodeIdEdgesMaxId(10, Array(11, 12, 13)),
    NodeIdEdgesMaxId(11, Array(12)),
    NodeIdEdgesMaxId(12, Array(11)),
    NodeIdEdgesMaxId(13, Array(14)),
    NodeIdEdgesMaxId(14, Array())
    ), StoredGraphDir.BothInOut, NeighborsSortingStrategy.LeaveUnsorted)

  def dangling_g7 = ArrayBasedDirectedGraph(Seq(
    NodeIdEdgesMaxId(1, Array(2)),
    NodeIdEdgesMaxId(2, Array(3, 4)),
    NodeIdEdgesMaxId(3, Array()),
    NodeIdEdgesMaxId(4, Array()),
    NodeIdEdgesMaxId(5, Array(6)),
    NodeIdEdgesMaxId(6, Array(1, 2, 7)),
    NodeIdEdgesMaxId(7, Array())
  ), StoredGraphDir.OnlyOut, NeighborsSortingStrategy.LeaveUnsorted)

  def dangling_g7_in = ArrayBasedDirectedGraph(Seq(
    NodeIdEdgesMaxId(1, Array(2)),
    NodeIdEdgesMaxId(2, Array(3, 4)),
    NodeIdEdgesMaxId(3, Array()),
    NodeIdEdgesMaxId(4, Array()),
    NodeIdEdgesMaxId(5, Array(6)),
    NodeIdEdgesMaxId(6, Array(1, 2, 7)),
    NodeIdEdgesMaxId(7, Array())
  ), StoredGraphDir.OnlyIn, NeighborsSortingStrategy.LeaveUnsorted)

  val nodeSeqIterator = Seq(
      NodeIdEdgesMaxId(10, Array(11, 12, 13)),
      NodeIdEdgesMaxId(11, Array(12, 14)),
      NodeIdEdgesMaxId(12, Array(14)),
      NodeIdEdgesMaxId(13, Array(12, 14)),
      NodeIdEdgesMaxId(14, Array(15)),
      NodeIdEdgesMaxId(15, Array(10, 11))
      )

  val nodeSeqIteratorWithEmpty = Seq(
    NodeIdEdgesMaxId(0, Array.empty[Int]),
    NodeIdEdgesMaxId(1, Array.empty[Int])
  ) ++ nodeSeqIterator

  // using testGraph becomes onerous for non-trivial graphs
  def g6 = ArrayBasedDirectedGraph(nodeSeqIterator, StoredGraphDir.BothInOut,
    NeighborsSortingStrategy.LeaveUnsorted)
  def g6_onlyout = ArrayBasedDirectedGraph(nodeSeqIterator, StoredGraphDir.OnlyOut,
    NeighborsSortingStrategy.LeaveUnsorted)
  def g6_onlyin = ArrayBasedDirectedGraph(nodeSeqIterator, StoredGraphDir.OnlyIn,
    NeighborsSortingStrategy.LeaveUnsorted)

  def g6WithEmptyNodes = ArrayBasedDirectedGraph(nodeSeqIteratorWithEmpty, StoredGraphDir.BothInOut,
    NeighborsSortingStrategy.LeaveUnsorted)

  val nodeSeqIterator2 = Seq(
      NodeIdEdgesMaxId(10, Array(11, 12, 13)),
      NodeIdEdgesMaxId(11, Array(10, 13, 14)),
      NodeIdEdgesMaxId(12, Array(13, 14)),
      NodeIdEdgesMaxId(13, Array(12, 14)),
      NodeIdEdgesMaxId(14, Array(10, 11, 15)),
      NodeIdEdgesMaxId(15, Array(10, 11, 16)),
      NodeIdEdgesMaxId(16, Array(15))
      )
  def g7_onlyout = ArrayBasedDirectedGraph(nodeSeqIterator2, StoredGraphDir.OnlyOut,
    NeighborsSortingStrategy.LeaveUnsorted)
  def g7_onlyin = ArrayBasedDirectedGraph(nodeSeqIterator2, StoredGraphDir.OnlyIn,
    NeighborsSortingStrategy.LeaveUnsorted)

  // Bipartite Graph
  def bipartiteGraphSingleSide = {
    /*
   lN -> 1 to 5
   rN -> 4,8,5,10,123,0
   1 --> i:(), o:()
   2 --> i: (), o: (5,10)
   3 --> i: (), o: ()
   4 --> i: (), o: (14)
   5 --> i: (), o: (5,10,8)
   */

    val leftNodes = new Array[BipartiteNode](6)
    leftNodes(1) = new LeftNode(1, Array.empty[Int], Array.empty[Int])
    leftNodes(2) = new LeftNode(2, Array.empty[Int], Array(5, 10))
    leftNodes(3) = new LeftNode(3, Array.empty[Int], Array.empty[Int])
    leftNodes(4) = new LeftNode(4, Array.empty[Int], Array(14))
    leftNodes(5) = new LeftNode(5, Array.empty[Int], Array(5, 10, 8))

    val rightNodes = new Array[BipartiteNode](124)
    rightNodes(14) = new RightNode(14, Array(4), Array.empty[Int])
    rightNodes(4) = new RightNode(4, Array.empty[Int], Array.empty[Int])
    rightNodes(5) = new RightNode(5, Array(2, 5), Array.empty[Int])
    rightNodes(8) = new RightNode(8, Array(5), Array.empty[Int])
    rightNodes(10) = new RightNode(10, Array(2, 5), Array.empty[Int])
    rightNodes(123) = new RightNode(123, Array.empty[Int], Array.empty[Int])

    val leftSide = BipartiteSide(leftNodes, 5, 6)
    val rightSide = BipartiteSide(rightNodes, 6, 0)

    new BipartiteGraph(leftSide, rightSide, BipartiteGraphDir.LeftToRight, false)
  }

  def bipartiteGraphDoubleSide = {
    /*
   lN -> 1 to 5
   rN -> 4,8,5,10,123,0
   1 --> i:(4,5,123,10), o:()
   2 --> i: (), o: (5,10)
   3 --> i: (), o: ()
   4 --> i: (14), o: (14)
   5 --> i: (4,10), o: (5,10,8)
   */

    val leftNodes = new Array[BipartiteNode](6)
    leftNodes(1) = new LeftNode(1, Array(4, 5, 123, 10), Array.empty[Int])
    leftNodes(2) = new LeftNode(2, Array.empty[Int], Array(5, 10))
    leftNodes(3) = new LeftNode(3, Array.empty[Int], Array.empty[Int])
    leftNodes(4) = new LeftNode(4, Array(14), Array(14))
    leftNodes(5) = new LeftNode(5, Array(4, 10), Array(5, 10, 8))

    val rightNodes = new Array[BipartiteNode](124)
    rightNodes(14) = new RightNode(14, Array(4), Array(4))
    rightNodes(4) = new RightNode(4, Array.empty[Int], Array(1, 5))
    rightNodes(5) = new RightNode(5, Array(2, 5), Array(1))
    rightNodes(8) = new RightNode(8, Array(5), Array.empty[Int])
    rightNodes(10) = new RightNode(10, Array(2, 5), Array(1, 5))
    rightNodes(123) = new RightNode(123, Array.empty[Int], Array(1))

    val leftSide = BipartiteSide(leftNodes, 5, 6)
    val rightSide = BipartiteSide(rightNodes, 6, 7)

    new BipartiteGraph(leftSide, rightSide, BipartiteGraphDir.Both, false)
  }

  def bipartiteGraphWithUniqueNodeIds = {

    val leftNodes = new Array[BipartiteNode](7)
    leftNodes(2) = new LeftNode(2, Array(1, 3, 5), Array(5))
    leftNodes(4) = new LeftNode(4, Array(3), Array(1))
    leftNodes(6) = new LeftNode(6, Array(5), Array(1, 3))

    val rightNodes = new Array[BipartiteNode](6)
    rightNodes(1) = new RightNode(1, Array(4, 6), Array(2))
    rightNodes(3) = new RightNode(3, Array(6), Array(2, 4))
    rightNodes(5) = new RightNode(5, Array(2), Array(2, 6))

    val leftSide = BipartiteSide(leftNodes, 3, 4)
    val rightSide = BipartiteSide(rightNodes, 3, 5)

    new BipartiteGraph(leftSide, rightSide, BipartiteGraphDir.LeftToRight, true)
  }

  // a complete graph is where each node follows every other node
  def generateCompleteGraph(numNodes: Int) = {
    val allNodes = (0 until numNodes).toList
    val testNodes = (0 until numNodes).toList map { source =>
      val allBut = allNodes.filter(_ != source)
      TestNode(source, allBut, allBut)
    }
    TestGraph(testNodes: _*)
  }

  /**
   * Computes random subsets of `range` such that number of elements
   * taken is sampled from `sizeDistribution`. Works in `O(p * n)` time.
   */
  private def randomSubset(sizeDistribution: BinomialDistribution, range: Range, rand: Random) : Array[Int] = {
    val positiveBitsNumber = sizeDistribution.sample(rand)
    Sampling.randomSubset(positiveBitsNumber, range, rand)
  }

  /**
   * @return Probability of existence of an edge in a random E-R graph.
   */
  def getProbEdgeRandomDirected(numNodes: Int, avgOutDegree: Int) = {
    require(numNodes > 1)
    avgOutDegree.toDouble / (numNodes - 1)
  }

  /**
   * @param numNodes number of nodes in the graph
   * @param probEdge probability of existence of an edge
   * @param graphDir store both directions or only one direction
   * @return a random Erdos-Renyi Directed graph
   */
  def generateRandomGraph(numNodes: Int, probEdge: Double, graphDir: StoredGraphDir = StoredGraphDir.BothInOut,
                          rand: Random = new Random) = {
    val nodes = new Array[NodeIdEdgesMaxId](numNodes)
    val binomialDistribution = new BinomialDistribution(numNodes - 1, probEdge)
    (0 until numNodes).par foreach { source =>
      val positiveBits = randomSubset(binomialDistribution, 0 until numNodes - 1, rand)
      val edgesFromSource = positiveBits map (x => if (x < source) x else x + 1)
      nodes(source) = NodeIdEdgesMaxId(source, edgesFromSource)
    }
    ArrayBasedDirectedGraph(nodes, graphDir,
      NeighborsSortingStrategy.LeaveUnsorted)
  }

  /**
   * @param numNodes number of nodes in the undirected graph
   * @param probEdge probability of existence of an edge, constant for each of (numNodes choose 2) edges
   * @param graphDir store both directions or only one direction
   * @return a random Erdos-Renyi undirected graph (a graph which only has mutual edges)
   */
  def generateRandomUndirectedGraph(numNodes: Int, probEdge: Double,
                                    graphDir: StoredGraphDir = StoredGraphDir.BothInOut,
                                    rand: Random = new Random,
                                    parallelismLimit: Int = 2) = {

    val futurePool = new BoundedFuturePool(FuturePool.unboundedPool, parallelismLimit)

    val nodes = Array.fill(numNodes){new ConcurrentLinkedQueue[Int]()}
    def addMutualEdge(i: Int)(j: Int) {nodes(i).add(j); nodes(j).add(i)}
    val binomialDistribution = new BinomialDistribution(numNodes - 1, probEdge)
    // Sampling edges only from nodes with lower id to higher id. In order to
    // reuse the same binomial distribution we match nodes in pairs, so that
    // one with id 0 is matched with one with id (n - 1), id 1 is matched with (n - 2)
    // and so on. Observe, that there is (n - 1) potential edges for every pair, that
    // connect from lower id node to higher. Thus for each pair we need to sample a vector
    // of Bernoulli variables of size (n - 1), from which we interprete first (lowerNode - 1)
    // bits as edges from higherNode and the rest from the node with lower id.
    val futures = (0 to (numNodes - 1) / 2) map {
      lowerNode => futurePool {
        val higherNode = numNodes - 1 - lowerNode
        val (higherNodeNeighbors, lowerNodeNeighbors) = randomSubset(binomialDistribution,
          0 until numNodes - 1, rand) partition (_ < lowerNode)
        lowerNodeNeighbors.map(_ + 1) foreach addMutualEdge(lowerNode)
        if (lowerNode != higherNode)
          higherNodeNeighbors map (higherNode + _ + 1) foreach addMutualEdge(higherNode)
      }
    }
    Await.ready(Future.join(futures))

    val nodesEdges = nodes.indices map { i =>
      NodeIdEdgesMaxId(i, nodes(i).asScala.toArray)
    }
    ArrayBasedDirectedGraph(nodesEdges, graphDir,
      NeighborsSortingStrategy.LeaveUnsorted)
  }
}
