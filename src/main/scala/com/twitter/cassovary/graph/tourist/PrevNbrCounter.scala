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
package com.twitter.cassovary.graph.tourist

import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}

/**
 * A NodeTourist that keeps track of the previous immediate neighbor of a
 * given node in visiting sequence.
 */
class PrevNbrCounter(numTopPathsPerNode: Option[Int], onlyOnce: Boolean)
    extends InfoKeeper[Array[(Int, Int)]] {

  def this() = this(None, false)

  /**
   * Keep info only the first time a node is seen
   */
  val infoPerNode = new Int2ObjectOpenHashMap[Int2IntOpenHashMap]

  /**
   * Record the previous neighbor {@code nodeId} of {@code id}.
   */
  def recordInfo(id: Int, nodeId: Int) {
    if (!(onlyOnce && infoPerNode.contains(id))) {
      if (!infoPerNode.containsKey(id)) {
        infoPerNode.put(id, new Int2IntOpenHashMap)
      }
      infoPerNode.get(id).add(nodeId, 1)
    }
  }

  private def topPrevNbrsTill(nodeId: Int, num: Option[Int]) = {
    val infoMap = infoPerNode.get(nodeId)
    val nbrsWithCounts = new Array[(Int, Int)](infoMap.size)
    var counter = 0

    val nodeIterator = infoMap.keySet.iterator
    while (nodeIterator.hasNext) {
      val nbrId = nodeIterator.nextInt
      nbrsWithCounts(counter) = (nbrId, infoMap.get(nbrId))
      counter += 1
    }

    val sorted = nbrsWithCounts.toSeq.sortBy { case (id, count) => -count }.toArray
    num match {
      case Some(n) => sorted.take(n)
      case None => sorted
    }
  }

  override def infoAllNodes = {
    val allPairs = new Array[(Int, Array[(Int, Int)])](infoPerNode.size)

    val nodeIterator = infoPerNode.keySet.iterator
    var counter = 0
    while (nodeIterator.hasNext) {
      val node = nodeIterator.nextInt
      allPairs(counter) = (node, topPrevNbrsTill(node, numTopPathsPerNode))
      counter += 1
    }

    allPairs
  }
}
