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
package com.twitter.cassovary.graph.bipartite

import com.twitter.cassovary.graph.{Graph, GraphDir, StoredGraphDir, Node}
import com.twitter.cassovary.graph.GraphDir._
import com.twitter.logging.Logger

case class BipartiteGraphException(e: String) extends Exception(e)

object BipartiteGraphDir extends Enumeration {
  type BipartiteGraphDir = Value
  val LeftToRight, RightToLeft, Both = Value
}
/*
 * BipartiteNode extends Node to have internalID and inNodes and outNodes
 * The id field inherited from Node is externalID in contrast.
 */
trait BipartiteNode extends Node {
  def isLeftNode: Boolean
  override def toString = {
    val leftOrRight = isLeftNode match {
      case true => "Left"
      case false => "Right"
    }
    leftOrRight + " node of className: " +
    this.getClass.getName + "\n" +
    "nodeId: " + id + "\n" +
    "inNodes:" + inboundNodes.toString + "\n" +
    "outNodes:" + outboundNodes.toString + "\n"
  }
}

/**
 * Represents a node on the LHS of a bipartite graph, with a negative node id,
 * all of its in and out edges point to nodes in the RHS, and thus all edge ids have positive values
 * @param id the id value of the node (it should be positive if all the node ids are unique in a graph
 *           and negative if node ids are unique only on each side)
 * @param inboundNodes original (positive) ids of the nodes on the RHS pointed by in-coming edges
 * @param outboundNodes original (positive) ids of the nodes on the RHS pointed by out-going edges
 */
class LeftNode(val id: Int, val inboundNodes: Seq[Int],
    val outboundNodes: Seq[Int]) extends BipartiteNode {
  def isLeftNode = true
}

/**
 * Represents a node on the RHS of a bipartite graph, with a positive node id,
 * all of its in and out edges point to nodes in the LHS, and thus all edge ids have values
 * of the real id * -1
 * @param id the id of the node (unique on the RHS)
 * @param inboundNodes ids of the nodes on the LHS pointed by in-coming edges
 * @param outboundNodes ids of the nodes on the LHS pointed by out-going edges
 * inboundNodes and outboundNodes ids should be positive if all the node idS are unique in a graph
 * and negative if node ids are unique only on each side.
 */
class RightNode(val id: Int, val inboundNodes: Seq[Int], val outboundNodes: Seq[Int]) extends BipartiteNode {
  def isLeftNode = false
}

case class BipartiteSide(nodes: Array[BipartiteNode], numOfNodes: Int, numOfOutEdges: Int)

/**
 * NOTE: this data structure could be memory intensive if the max node ID is much larger
 * than the number of elements in the graph, consider using alternative storage.
 * This class represents an array-based bipartite graph. All its nodes are BipartiteNodes.
 * It maintains two types of ID systems: left node ids (< 0) and right node ids (> 0), id value of 0
 * is not allowed in a Bipartite Graph.
 * Internally it stores two separate arrays, one for left nodes and one for right nodes.
 * The length of the array is the maxNodeId + 1.
 * When a node is being queries, based on the ID value, it converts the ID value to index
 * in the array * (in case of Right, identical; in case of left, the index is id * -1),
 * and retrieves the node object.
 * Edges in the graph fall into two types: from left to right, or from right to left.
 * For example, an edge E that goes from LeftNode A to RightNode B
 * could indicate membership relation.
 * From A's perspective, this is an out-going edge to B, which means A includes B as a member;
 * and similarly from B's perspective, this is an in-coming edge from A, meaning B is included
 * as a member in A.  * Another edge E' that goes from B to A, for example, could carry completely
 * different meaning, e.g. followship.
 * From B's perspective, E' is an out-going edge to A, and means that B follows A, and vice
 * versa from A's perspective. E' is an incoming edge, meaning A is followed by B.
 * If a BipartiteGraph is of direction BipartiteLeftToRight or BipartiteRightToLeft, it carries one
 * semantic meaning between the left and right sides; but if the graph is of direction
 * BipartiteBoth, then it could carry two semantic
 * meanings in one graph (e.g. list-to-user membership and user-to-list followship).
 * @param nodeIdsUnique true if node ids are unique in the whole graph
 *                      and false if unique only on each side (LHS and RHS)
 * If the node ids are unique only on each side, then the Left Nodes ids are negated,
 * so that ids are unique in the whole graph. If efficiency is desired, it is best to have node ids
 * unique in the whole graph.
 */
class BipartiteGraph(leftBipartiteNodes: Array[BipartiteNode], val leftNodeCount: Int,
                     val leftOutEdgeCount: Long, rightBipartiteNodes: Array[BipartiteNode],
                     val rightNodeCount: Int, val rightOutEdgeCount: Long,
                     val bipartiteGraphDir: BipartiteGraphDir.BipartiteGraphDir,
                     val nodeIdsUnique: Boolean = true) extends Graph[BipartiteNode] {

  def this(leftSide: BipartiteSide, rightSide: BipartiteSide,
      bipartiteGraphDir: BipartiteGraphDir.BipartiteGraphDir) =
    this(leftSide.nodes, leftSide.numOfNodes, leftSide.numOfOutEdges,
         rightSide.nodes, rightSide.numOfNodes, rightSide.numOfOutEdges,
         bipartiteGraphDir, true)

  def this(leftSide: BipartiteSide, rightSide: BipartiteSide,
           bipartiteGraphDir: BipartiteGraphDir.BipartiteGraphDir,
           nodeIdsUnique: Boolean) =
     this(leftSide.nodes, leftSide.numOfNodes, leftSide.numOfOutEdges,
       rightSide.nodes, rightSide.numOfNodes, rightSide.numOfOutEdges,
       bipartiteGraphDir, nodeIdsUnique)

  private val log = Logger.get
  val storedGraphDir =  StoredGraphDir.Bipartite

  val leftNodes = {
    if (!nodeIdsUnique)
      leftBipartiteNodes map { node =>
        if (node != null) new LeftNode(-node.id, node.inboundNodes(), node.outboundNodes())
        else null
      }
    else leftBipartiteNodes
  }
  
  val rightNodes = {
    if (!nodeIdsUnique)
      rightBipartiteNodes map { node =>
        if (node != null) {
          for (i <- 0 until node.inboundNodes().length) {
            if (node.inboundNodes()(i) == 0) throw new BipartiteGraphException(
              "Edge value cannot be 0, node %d's in-edge at edge index %d".format(node.id, i))
          }
          for (i <- 0 until node.outboundNodes().length) {
            if (node.outboundNodes()(i) == 0) throw new BipartiteGraphException(
              "Edge value cannot be 0, node %d's out edge at edge index %d".format(node.id, i))
          }
          new RightNode(node.id, node.inboundNodes().map(x => -x), node.outboundNodes().map(x => -x))
        } else null
      }
    else rightBipartiteNodes
  }

  /**
   * Checks for a given node, is a graph direction is stored.
   * e.g., if the node being queries is a left node, and the asked for direction is OutDir,
   * then if the graph's stored direction is LeftToRight or Both, then this function returns true,
   * false otherwise.
   */
  def isDirStored(node: BipartiteNode, dir: GraphDir) = {
    if (bipartiteGraphDir == BipartiteGraphDir.Both) true
    else {
      (node.isLeftNode, dir) match {
        case (true, GraphDir.InDir) => bipartiteGraphDir == BipartiteGraphDir.RightToLeft
        case (true, GraphDir.OutDir) => bipartiteGraphDir == BipartiteGraphDir.LeftToRight
        case (false, GraphDir.InDir) => bipartiteGraphDir == BipartiteGraphDir.LeftToRight
        case (false, GraphDir.OutDir) => bipartiteGraphDir == BipartiteGraphDir.RightToLeft
        case (_, _) => false
      }
    }
  }

  /**
   * Get the Bipartite node in the graph by a given internal (unique) id
   */
  def getNodeById(id: Int): Option[BipartiteNode] = {
    def isLeftNodeId: Boolean = id < 0

    if (!nodeIdsUnique) {
      if (id == 0) {
        return None // id 0 is not allowed when nodes are not unique in bipartite graph
      } else if (isLeftNodeId) {
        //invert nodeId to index
        val leftOriginalId = -id
        returnNodeIfPresentInGraph(leftOriginalId, Some(false))
      } else {
        returnNodeIfPresentInGraph(id, Some(true))
      }
    } else {
      returnNodeIfPresentInGraph(id)
    }
  }

  /**
   * Return Node if it is Present in graph
   * @param id
   * @param rightSide true if node is present in RHS, false if LHS and none if not sure yet
   */
  private def returnNodeIfPresentInGraph(id: Int, rightSide: Option[Boolean] = None): Option[BipartiteNode] = {
    rightSide match {
      case Some(true) =>
        if (id < rightNodes.length && rightNodes(id) != null) Some(rightNodes(id))
        else None

      case Some(false) =>
        if (id < leftNodes.length && leftNodes(id) != null) Some(leftNodes(id))
        else None

      case None =>
        if (id < leftNodes.length && leftNodes(id) != null) Some(leftNodes(id))
        else if (id < rightNodes.length && rightNodes(id) != null) Some(rightNodes(id))
        else None
    }
  }
}
