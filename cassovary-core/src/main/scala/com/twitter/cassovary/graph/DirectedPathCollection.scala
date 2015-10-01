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

import com.twitter.cassovary.util.collections.{CQueue, Order}
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.objects._

/**
 * Represents a collection of directed paths. For example, a collection of paths out of
 * the same source node.
 */
class DirectedPathCollection {

  // the current path being built
  private val currPath = DirectedPath.builder()

  // pathCountsPerId.get(id).get(path) = count of #times path has been seen ending in id
  private val pathCountsPerId = new Int2ObjectOpenHashMap[Object2IntOpenHashMap[DirectedPath]]

  /**
   * Priority queue and comparator for sorting top paths. Reused across nodes.
   */
  val comparator = new PathCounterComparator(pathCountsPerId, true)
  val priQ = CQueue.priority[DirectedPath](comparator)

  /**
   * Appends node to the current path and record this path against the node.
   * @param node the node being visited
   */
  def appendToCurrentPath(node: Int) {
    currPath.append(node)
    pathCountsPerIdWithDefault(node).addTo(currPath.snapshot, 1)
  }

  /**
   * Reset the current path
   */
  def resetCurrentPath() {
    currPath.clear()
  }

  /**
   * @return a map of {path -> count} for paths ending at `node`, sorted in decreasing order of count.
   *         Paths with the same count are sorted shortest path first.
   *
   *         Returns at most `num` pairs in the map.
   */
  def topPathsTill(node: Int, num: Int): Object2IntMap[DirectedPath] = {
    val pathCountMap = pathCountsPerIdWithDefault(node)
    val pathCount = pathCountMap.size
    val returnMap = new Object2IntArrayMap[DirectedPath]

    comparator.setNode(node)
    priQ.clear()

    val pathIterator = pathCountMap.keySet.iterator
    while (pathIterator.hasNext) {
      val path = pathIterator.next()
      priQ += path
    }

    var size = 0
    while (size < num && !priQ.isEmpty) {
      val path = priQ.deque()
      returnMap.put(path, pathCountMap.get(path))
      size += 1
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
   * @return the total number of unique paths that end at `node`
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
   * @return true if entry for `node` exists, false otherwise
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

class PathCounterComparator(pathCountsPerId: Int2ObjectOpenHashMap[Object2IntOpenHashMap[DirectedPath]], descending: Boolean)
  extends Order[DirectedPath] {

  var infoMap: Object2IntOpenHashMap[DirectedPath] = null

  def setNode(id: Int) {
    infoMap = pathCountsPerId.get(id)
  }

  /**
   * Compares paths using counts and (if equal) lengths (reversed).
   */
  override def compare(dp1: DirectedPath, dp2: DirectedPath): Int = {
    val dp1Count = infoMap.getInt(dp1)
    val dp2Count = infoMap.getInt(dp2)

    if (dp1Count != dp2Count) {
      if (descending) {
        dp2Count - dp1Count
      } else {
        dp1Count - dp2Count
      }
    } else {
      if (descending) {
        dp1.length - dp2.length
      } else {
        dp2.length - dp1.length
      }
    }
  }
}
