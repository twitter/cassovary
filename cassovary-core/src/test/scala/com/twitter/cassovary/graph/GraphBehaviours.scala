/*
 * Copyright (c) 2014. Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.util.NodeNumberer
import org.scalatest.{Matchers, WordSpec}

trait GraphBehaviours[V <: Node] extends Matchers {
  this: WordSpec =>

  private def correctNumberOfNodesAndEdges(graph: DirectedGraph[V], numNodes: Int) {
    "have correct number of nodes" in {
      graph.nodeCount shouldEqual numNodes
    }
    "have number of edges less than in complete graph" in {
      val mul = if (graph.isBiDirectional) 2 else 1
      graph.edgeCount should be <= (mul * numNodes * (numNodes - 1).toLong)
    }
  }

  def completeGraph(graph: => DirectedGraph[V], numNodes: Int) {
    "have correct number of nodes" in {
      graph.nodeCount shouldEqual numNodes
    }
    "have all possible edges" in {
      val mul = if (graph.isBiDirectional) 2 else 1
      graph.edgeCount shouldEqual (mul * numNodes * (numNodes - 1).toLong)
      graph foreach { node =>
        val neighbors = (0 until numNodes) filter {
          _ != node.id
        }
        node.inboundNodes().toSeq.sorted shouldEqual neighbors
        node.outboundNodes().toSeq.sorted shouldEqual neighbors
      }
    }
  }

  def correctDirectedGraph(graph: => DirectedGraph[V], numNodes: Int) {
    correctNumberOfNodesAndEdges(graph, numNodes)

    "be consistent among all nodes" in {
      graph foreach { node =>
        (0 to 1) foreach { dir =>
          val neighbors = if (dir == 0) node.outboundNodes() else node.inboundNodes()
          withClue("duplicates in neighbors array detected") {
            neighbors.length shouldEqual neighbors.toSeq.toSet.size
          }
          neighbors foreach { nbr =>
            withClue("nbr.id < 0 (" + nbr + ")") {
              nbr should be >= 0
            }
            withClue("nbr.id >= numNodes (" + nbr + ")") {
              nbr should be < numNodes
            }
            withClue("self-loop in graph") {
              nbr should not be node.id
            }
            val neighborNode = graph.getNodeById(nbr).get
            val neighborNodeOtherDirEdges =
              if (dir == 0) neighborNode.inboundNodes()
              else neighborNode.outboundNodes()
            withClue("edge existence not consistent in nodes") {
              neighborNodeOtherDirEdges.toSeq should contain (node.id)
            }
          }
        }
      }
    }
  }

  val sampleGraphEdges = Map(1 -> Seq(2,3,4), 2 -> Seq(1), 3 -> Seq(4), 5 -> Seq(1, 10))

  def verifyInOutEdges(graph: DirectedGraph[V], numNodes: Int,
                       outEdges: Map[Int, Seq[Int]],
                       inEdges: Map[Int, Seq[Int]],
                       checkOrdering: Boolean = false): Unit = {

    def check(id: Int, a: Seq[Int], b: Option[Seq[Int]]) {
      if (a.isEmpty) {
        withClue("graph's nodeid " + id + " is empty but supplied edges is not: " + b) {
          b should (be ('empty) or be(Some(Array.empty[Int])))
        }
      } else {
        withClue("graph's nodeid " + id + " is not empty but supplied edges is") {
          b should not be 'empty
        }
        if (checkOrdering) {
          withClue("graph's nodeid " + id + "'s edges do not match: in graph = " +
            a + " edges = " + b.get + "\n") {
            a shouldEqual b.get
          }
        } else {
          withClue("graph's nodeid " + id + "'s edges do not match: in graph = " +
            a.toSet + " edges = " + b.get.toSet + "\n") {
            a.toSet shouldEqual b.get.toSet
          }
        }
      }
    }

    correctNumberOfNodesAndEdges(graph, numNodes)

    "have correct in and out edges" + (if (checkOrdering) " in correct order" else "") in {
      for (node <- graph) {
        check(node.id, node.outboundNodes().toSeq, outEdges.get(node.id))
        check(node.id, node.inboundNodes().toSeq, inEdges.get(node.id))
      }
    }
  }

  // verify a graph constructed from a supplied map of node id -> array of edges (outedges unless
  // OnlyIn direction is to be stored in which case the supplied edges are incoming edges)
  def verifyGraphBuilding(builder: (Iterable[NodeIdEdgesMaxId], StoredGraphDir) => DirectedGraph[V],
                  givenEdges: Map[Int, Seq[Int]]): Unit =
  {
    def cross(k: Int, s: Seq[Int]) = for (e <- s) yield (e, k)

    val allIds: Set[Int] = givenEdges.keys.toSet ++ givenEdges.values.toSet.flatMap { x: Seq[Int] => x }
    val noEdges = Map.empty[Int, Seq[Int]]
    def iterableSeq = givenEdges map { case (k, s) => NodeIdEdgesMaxId(k, s.toArray) }
    for (dir <- List(StoredGraphDir.BothInOut, StoredGraphDir.OnlyOut, StoredGraphDir.OnlyIn)) {
      val graph = builder(iterableSeq, dir)
      "Graph constructed in direction " + dir should {
        val inEdges: Map[Int, Seq[Int]] = dir match {
          case StoredGraphDir.OnlyIn => givenEdges
          case StoredGraphDir.OnlyOut => noEdges
          case StoredGraphDir.BothInOut =>
            givenEdges.toArray flatMap { case (k, s) => cross(k, s) } groupBy(_._1) mapValues {_.map(_._2) }
        }
        verifyInOutEdges(graph, allIds.size,
          if (dir == StoredGraphDir.OnlyIn) noEdges else givenEdges, inEdges)
      }
    }
  }

  def correctUndirectedGraph(graph: => DirectedGraph[V], numNodes: Int) {
    correctNumberOfNodesAndEdges(graph, numNodes)

    "be consistent among all nodes" in {
      graph foreach { node =>
        withClue("inbound nodes not equal to outbound nodes in undirected graph") {
          node.inboundNodes().toSeq.sorted shouldEqual node.outboundNodes().toSeq.sorted
        }
        node.inboundNodes().length shouldEqual node.inboundNodes().toSeq.toSet.size
        node.inboundNodes() foreach { nbr =>
          withClue("nbr.id < 0 (" + nbr + ")") {
            nbr should be >= 0
          }
          withClue("nbr.id >= numNodes (" + nbr + ")") {
            nbr should be < numNodes
          }
          withClue("node " + node.id + " contains self-loop") {
            nbr should not be node.id
          }
          withClue("edge existence not consistent in nodes") {
            graph.getNodeById(nbr).get.outboundNodes().toSeq should contain (node.id)
          }
        }
      }
    }
  }

  def graphEquivalentToMap(g: DirectedGraph[V], nodeMap: Map[Int, List[Int]]) = {
    g.foreach {
      node =>
        withClue("unexpected node: " + node.id) {
          nodeMap.contains(node.id) shouldEqual true
        }
        val neighbors = node.outboundNodes().toSeq.toArray.sorted
        val nodesInMap = nodeMap(node.id).toArray.sorted
        withClue("node: " + node.id + " neighbors.length incorrect") {
          neighbors.length shouldEqual nodesInMap.length
        }
        neighbors.iterator.zip(nodesInMap.iterator).foreach {
          case (a, b) => withClue("node: " + node.id + " neighbors incorrect") { a shouldEqual b }
        }
    }
  }

  /**
   * Compares the nodes in a graph and those defined by the nodeMap (id -> ids of neighbors),
   * remapping node ids thru nodeRenumberer, and ensures that these are equivalent
   * @param g DirectedGraph
   * @param nodeMap Map of node ids to ids of its neighbors
   * @param nodeNumberer a node renumberer
   */
  def renumberedGraphEquivalentToMap[T](g: DirectedGraph[V], nodeMap: Map[T, List[T]], nodeNumberer: NodeNumberer[T]) = {
    g.foreach { node =>
      withClue("unexpected node in a graph: " + nodeNumberer.internalToExternal(node.id)) {
        nodeMap.contains(nodeNumberer.internalToExternal(node.id)) shouldEqual true
      }
      val neighbors = node.outboundNodes()
      val nodesInMap = nodeMap(nodeNumberer.internalToExternal(node.id))
      withClue("edge missing in the graph") {
        nodesInMap
          .forall { i =>
            neighbors.toSeq.contains(nodeNumberer.externalToInternal(i)) } shouldEqual true
      }
      withClue("unexpected edge in the graph") {
        neighbors.toSeq
          .forall { i => nodesInMap.contains(nodeNumberer.internalToExternal(i)) } shouldEqual true
      }
    }
    nodeMap.keys.foreach {
      id => withClue("node " + id + " missing in graph") {
        g.existsNodeId(nodeNumberer.externalToInternal(id)) shouldEqual true
      }
    }
  }
}
