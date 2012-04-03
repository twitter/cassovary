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

import it.unimi.dsi.fastutil.objects._
import it.unimi.dsi.fastutil.ints._
import java.util.Comparator


/**
 * Represents a collection of directed paths. For example, a collection of paths out of
 * the same source node. This class is *not* thread safe!
 */
class DirectedPathCollection {

  // the current path being built
  private val currPath = DirectedPath.builder()

  // pathCountsPerId.get(id).get(path) = count of #times path has been seen ending in id
  private val pathCountsPerId = new Int2ObjectOpenHashMap[Object2IntOpenHashMap[DirectedPath]]

  /**
   * Priority queue and comparator for sorting top paths. Reused across nodes.
   * Synchronized for thread safety.
   */
  val comparator = new PathCounterComparator(pathCountsPerId, true)
  val priQ = new ObjectArrayPriorityQueue[DirectedPath](comparator)

  /**
   * Appends node to the current path and record this path against the node.
   * @param node the node being visited
   */
  def appendToCurrentPath(node: Int) {
    currPath.append(node)
    pathCountsPerIdWithDefault(node).add(currPath.snapshot, 1)
  }

  /**
   * Reset the current path
   */
  def resetCurrentPath() {
    currPath.clear()
  }

  /**
   * @return an map of top DirectedPaths with occurrence
   *         ending at {@code node}, sorted decreasing
   */
  def topPathsTill(node: Int, num: Int): Object2IntMap[DirectedPath] = {
    val pathCountMap = pathCountsPerIdWithDefault(node)
    val pathCount = pathCountMap.size
    val returnMap = new Object2IntArrayMap[DirectedPath]

    priQ.synchronized {
      comparator.setNode(node)
      priQ.clear()

      val pathIterator = pathCountMap.keySet.iterator
      while (pathIterator.hasNext) {
        val path = pathIterator.next
        priQ.enqueue(path)
      }

      while (returnMap.size < num && !priQ.isEmpty) {
        val path = priQ.dequeue()
        returnMap.put(path, pathCountMap.get(path))
      }
    }

    returnMap
  }

  /**
   * @param num the number of top paths to return for a node
   * @return an array of tuples, each containing a node and array of top paths ending at node, with scores
   */
  def topPathsPerNodeId(num: Int): Int2ObjectOpenHashMap[Object2IntMap[DirectedPath]] = {
    val topPathMap = new Int2ObjectOpenHashMap[Object2IntMap[DirectedPath]]

    val nodeIterator = pathCountsPerId.keySet.iterator
    while (nodeIterator.hasNext) {
      val node = nodeIterator.nextInt
      topPathMap.put(node, topPathsTill(node, num))
    }

    topPathMap
  }

  /**
   * @return the total number of unique paths that end at {@code node}
   */
  def numUniquePathsTill(node: Int) = pathCountsPerIdWithDefault(node).size

  /**
   * @return the total number of unique paths in this collection
   */
  def totalNumPaths = {
    var sum = 0
    val iterator = pathCountsPerId.keySet.iterator
    while (iterator.hasNext) {
      val node = iterator.nextInt
      sum += numUniquePathsTill(node)
    }
    sum
  }

  /**
   * @return true if entry for {@code node} exists, false otherwise
   */
  def containsNode(node: Int): Boolean = {
    pathCountsPerId.containsKey(node)
  }

  /**
   * Resets the path collection
   */
  def clear() {
    currPath.clear()
    pathCountsPerId.clear()
  }

  private def pathCountsPerIdWithDefault(node: Int): Object2IntOpenHashMap[DirectedPath] = {
    if (!pathCountsPerId.containsKey(node)) {
      val map = new Object2IntOpenHashMap[DirectedPath]
      pathCountsPerId.put(node, map)
    }
    pathCountsPerId.get(node)
  }

}

class PathCounterComparator(pathCountsPerId: Int2ObjectOpenHashMap[Object2IntOpenHashMap[DirectedPath]], descending: Boolean) extends Comparator[DirectedPath] {

  var infoMap: Object2IntOpenHashMap[DirectedPath] = null

  def setNode(id: Int) {
    infoMap = pathCountsPerId.get(id)
  }

  override def compare(dp1: DirectedPath, dp2: DirectedPath): Int = {
    val dp1Count = infoMap.getInt(dp1)
    val dp2Count = infoMap.getInt(dp2)
    if (descending) {
      dp2Count - dp1Count
    } else {
      dp1Count - dp2Count
    }
  }
}
