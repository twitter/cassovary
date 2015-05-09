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

import scala.util.Random

/**
 *   Represents a node in a directed graph.
 */
trait Node {
  import com.twitter.cassovary.graph.GraphDir._

  /**
   * The unique id of this node.
   */
  val id: Int

  /**
   * Returns ids of all nodes pointing to this node.
   */
  def inboundNodes(): Seq[Int]

  /**
   * Returns up to `max` nodes that this node points to.
   * @param max the max number of nodes it needs
   * @return a sequence of inboundNode ids
   */
  def inboundNodes(max: Int): Seq[Int] = inboundNodes.take(max)

  /**
   * Returns a random node from the set of nodes that points to this node or else `None` if
   * this node has no inbound edges.
   *
   * The default implementation picks a random node from `inboundNodes()` so subclasses
   * should consider overriding this method if the `Seq` sequence they produce is not
   * a `IndexedSeq`.
   *
   * @return a sequence of random node ids
   */
  def randomInboundNode: Option[Int] = randomInboundNode(Node.randGen)

  /**
   * Returns a random node from the set of nodes that points to this node or else `None` if
   * this node has no inbound edges, using the supplied random number generator `rnd`.
   * @param rnd user defined random number generator
   * @return a random node id
   */
  def randomInboundNode(rnd: Random) = randomNode(inboundNodes, rnd)

  /**
   * Returns a random sample of size at most `numResults` from the set of nodes
   * that point to this node using the supplied random number generator `rnd`.
   * @param numResults max number of random nodes needed
   * @param rnd user defined random number generator
   * @return a set of random node id
   */
  def randomInboundNodeSet(numResults: Int, rnd: Random): Seq[Int] =
    randomNodeSet(inboundNodes, numResults, rnd)

  /**
   * Returns `true` if the given `nodeId` points to this node.
   * @param nodeId host node id
   * @return a set of random node id
   */
  def isInboundNode(nodeId: Int): Boolean = containsNode(inboundNodes, nodeId)

  /**
   * @return the total number of inbound edges.
   */
  def inboundCount: Int = inboundNodes.size

  /**
   * @return all nodes this node points to.
   */
  def outboundNodes(): Seq[Int]

  /**
   * @param max the maximum number of outBound nodes needed.
   * @return up to `max` nodes that this node points to.
   */
  def outboundNodes(max: Int): Seq[Int] = outboundNodes.take(max)

  /**
   * Returns a random node from the set of nodes that this node points to or else `None` if
   * this node has no outbound edges.
   *
   * The default implementation picks a random node from `outboundNodes()` so subclasses
   * should consider overriding this method if the `Seq` sequence they produce is not
   * a lazy `IndexedSeq`.
   *
   * @return a random node that this node points to.
   */
  def randomOutboundNode: Option[Int] = randomOutboundNode(Node.randGen)

  /**
   * Returns a random node from the set of nodes that this node points to or else `None` if
   * this node has no outbound edges, using the supplied random number generator `rnd`.
   * @param rnd a user defined random number generator.
   * @return a random node that this node points to.
   */
  def randomOutboundNode(rnd: Random) = randomNode(outboundNodes, rnd)

  /**
   * Returns a random sample of size at most `numResults` from the set of nodes
   * that this node points to using the supplied random number generator `rnd`.
   * @param rnd a user defined random number generator.
   * @return a set of random nodes that this node points to.
   */
  def randomOutboundNodeSet(numResults: Int, rnd: Random): Seq[Int] =
      randomNodeSet(outboundNodes, numResults, rnd)

  /**
   * Returns `true` if the this node point to the given `node`.
   * @param nodeId home node id
   * @return a boolean indicating whether outbound nodes contains `nodeId`.
   */
  def isOutboundNode(nodeId: Int): Boolean = containsNode(outboundNodes, nodeId)

  /**
   * @return the total number of outbound edges.
   */
  def outboundCount: Int = outboundNodes.size

  /**
   * A method that return either inbound or outbound allowing direction `dir`.
   * @param dir the direction (inbound or outbound) that the method is applied to.
   * @return a sequence of inbound or outbound neighbors.
   */
  def neighborIds(dir: GraphDir): Seq[Int] = {
    dir match {
      case OutDir => outboundNodes
      case InDir => inboundNodes
    }
  }

  /**
   * A method that returns `max` nodes of either inbound or outbound
   * allowing direction `dir`.
   * @param dir the direction (inbound or outbound) that the method is applied to.
   * @param max the maximum number of neighbors needed.
   * @return a sequence of inbound or outbound neighbors.
   */
  def neighborIds(dir: GraphDir, max: Int): Seq[Int] = neighborIds(dir).take(max)

  /**
   * A method that returns a random node in the allowing direction `dir`.
   * @param dir the direction (inbound or outbound) that the method is applied to.
   * @return a random neighbor or `None` if no neighbor is in the direction `dir`.
   */
  def randomNeighbor(dir: GraphDir): Option[Int] = randomNeighbor(dir, Node.randGen)

  /**
   * A method that returns a random node in the allowing direction `dir`,
   * using the supplied random number generator `rnd`.
   * @param dir the direction (inbound or outbound) that the method is applied to.
   * @param rnd a user defined random number generator.
   * @return a random neighbor or `None` if no neighbor is in the direction `dir`.
   */
  def randomNeighbor(dir: GraphDir, rnd: Random): Option[Int] = {
    dir match {
      case OutDir => randomOutboundNode(rnd)
      case InDir => randomInboundNode(rnd)
    }
  }

  /**
   * A method that returns a random node of size `numResults` in the allowing direction `dir`,
   * using the supplied random number generator `rnd`.
   * @param numResults maximum number of neighbors needed.
   * @param dir the direction (inbound or outbound) that the method is applied to.
   * @return a set of random neighbors.
   */
  def randomNeighborSet(numResults: Int, dir: GraphDir): Seq[Int] =
    randomNeighborSet(numResults, dir, Node.randGen)


  /**
   * A method that returns a set of either inbound or outbound nodes of size `numResults`,
   * in the allowing direction `dir`, using the supplied random number
   * generator `rnd`.
   * @param numResults maximum number of neighbors needed.
   * @param dir the direction (inbound or outbound) that the method is applied to.
   * @param rnd a user defined random number generator.
   * @return a set of random neighbors.
   */
  def randomNeighborSet(numResults:Int, dir: GraphDir, rnd: Random) = {
    dir match {
      case OutDir => randomOutboundNodeSet(numResults, rnd)
      case InDir => randomInboundNodeSet(numResults, rnd)
    }
  }

  /**
   * whether `nodeId` is a neighbor in the allowing direction `dir`
   * @param dir the direction (inbound or outbound) that the method is applied to.
   * @param nodeId the target node id.
   * @return a boolean indicating whether `nodeId` is in the home node's neighbors.
   */
  def isNeighbor(dir: GraphDir, nodeId: Int): Boolean = {
    dir match {
      case OutDir => isOutboundNode(nodeId)
      case InDir => isInboundNode(nodeId)
    }
  }

  /**
   * @return Intersection of `dir` neighbors with `nodeIds`.
   */
  def intersect(dir: GraphDir, nodeIds: Seq[Int]): Seq[Int] = {
    intersect(neighborIds(dir), nodeIds)
  }

  protected def intersect(neighbors: Seq[Int], nodeIds: Seq[Int]): Seq[Int] = {
    neighbors.intersect(nodeIds)
  }

  /**
   * the neighbor count in the allowing direction `dir`
   * @param dir the direction (inbound or outbound) that the method is applied to.
   * @return the number of neighbors in the direction of `dir`.
   */
  def neighborCount(dir: GraphDir): Int = {
    dir match {
      case OutDir => outboundCount
      case InDir => inboundCount
    }
  }

  /**
   * The default implementation just walks the array but users likely want to override to provide a
   * more optimized implementation.
   * @return a boolean indicating Whether `nodeIds` contains `queryNodeId`.
   */
  protected def containsNode(nodeIds: Seq[Int], queryNodeId: Int): Boolean = {
    nodeIds.contains(queryNodeId)
  }

  /**
   * @param rnd a user defined random number generator.
   * @return a random node from `nodeIds` using a supplied random number generator `rnd`.
   */
  protected def randomNode(nodeIds: Seq[Int], rnd: Random): Option[Int] = {
    if (nodeIds.isEmpty) {
      None
    } else {
      Some(nodeIds(rnd.nextInt(nodeIds.length)))
    }
  }

  /**
   * Random sampling with replacement.  Choose a set of random nodes of size `numResults`
   * from `nodeIds` using a supplied random number generator `rnd`.
   * @param numResults maximum number of nodes needed.
   * @param rnd a user defined random number generator.
   * @return a random node from `nodeIds` using a supplied random number generator `rnd`.
   */
  protected def randomNodeSet(nodeIds: Seq[Int], numResults: Int, rnd: Random) = {
    val arraySize = if (nodeIds.isEmpty) {0} else {numResults}
    (1 to arraySize).map(_ => nodeIds(rnd.nextInt(nodeIds.size))).toArray
  }

  /**
   * Override toString to make debugging easier. It prints max of 10 neighbors in each direction.
   */
   override def toString  = {
     "NodeId => " + id + "\n" +
     inboundNodes.take(10).foldLeft("InboundNodes[" + inboundCount + "] =>") { (accum, node) =>
       accum + node + "|"} + "\n" +
     outboundNodes.take(10).foldLeft("OutboundNodes[" + outboundCount + "] =>"){ (accum, node) =>
       accum + node + "|"} + "\n"
   }
}

object Node {
  private lazy val randGen: Random = new Random

  def apply(nodeId: Int, in: Seq[Int], out: Seq[Int]): Node = {
    new SeqBasedNode(nodeId, in, out)
  }

  def withSortedNeighbors(nodeId: Int, in: Array[Int], out: Array[Int]) = {
    new SeqBasedNode(nodeId, in, out) with SortedNeighborsNodeOps
  }
}

/**
 * Constructor for a default node with neighbors stored as Seqs.
 */
class SeqBasedNode private[graph] (val id: Int, val inboundNodes: Seq[Int], val outboundNodes: Seq[Int])
  extends Node
