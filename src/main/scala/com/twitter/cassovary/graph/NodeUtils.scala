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

import com.twitter.cassovary.graph.GraphDir._
import scala.util.Random

/**
 * This class contains common graph node based utilities and convenience functions.
 * In general, only utilities that are local to a node are kept here. Utility methods
 * that touch many nodes are found in {@link GraphUtils}.
 */
object NodeUtils {
  /**
   * Removes {@code node} and those nodes that {@code node} directly follows from a given set
   * @param node the relevant node
   * @param allNodeInfos the information about a set of nodes
   * @param extractIdFunc function to extract the id from the information about a node
   * @return {@code allNodeInfos} without {@code node} and nodes directly followed by {@code node}
   */
  def removeSelfAndNodesDirectlyFollowing[T](node: Node, allNodeInfos: List[T],
      extractIdFunc: T => Int) = {
    removeFromList(Set(node.outboundNodes: _*) ++ Set(node.id), allNodeInfos, extractIdFunc)
  }

  /**
   * Removes from a given universe of elements, those that belong to {@code discardSet}
   * @param discardSet set of ints identifying elements that need to be discarded
   * @param universe the list of all elements
   * @param extractFunc function to extract the int from one element in the universe
   * @return {@code universe} without those elements that are present in {@code discardSet}
   */
  def removeFromList[T](discardSet: Set[Int], universe: List[T], extractFunc: T => Int) = {
    universe filter { el =>
      val i = extractFunc(el)
      !discardSet.contains(i)
    }
  }

  /**
   * Checks if a node has too many edges.
   * @param thresh the threshold (maximum value to be checked against)
   * @param node the relevant node
   */
  def hasTooManyEdges(dir: GraphDir, thresh: Option[Int])(node: Node) = thresh match {
    case None => false
    case Some(n) => (node.neighborCount(dir) > n)
  }

  /**
   * Returns the id of some random home node
   * @param nodeIds  the list of home node ids
   * @param randNumGen  some random number generator
   * @return the id of some random home node
   */
  def pickRandNodeId(nodeIds: Seq[Int], randNumGen: Random): Int = {
    nodeIds(randNumGen.nextInt(nodeIds.length))
  }
}
