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
package com.twitter.cassovary.graph.tourist

import it.unimi.dsi.fastutil.ints._
import java.{util => jutil}
import scala.collection.JavaConversions._

/**
 * A NodeTourist that keeps track of the previous immediate neighbor of a
 * given node in visiting sequence.
 */
class PrevNbrCounter(val numTopPathsPerNode: Option[Int], override val onlyOnce: Boolean)
    extends InfoKeeper[Int2IntMap] {

  /**
   * Keep info only the first time a node is seen
   */
  def this() = this(None, false)

  protected val underlyingMap = new Int2ObjectOpenHashMap[Int2IntOpenHashMap]

  override def infoPerNode = underlyingMap.asInstanceOf[jutil.Map[Int, Int2IntMap]]

  /**
   * Priority queue and comparator for sorting prev nbrs. Reused across nodes.
   */
   val comparator = new PrevNbrComparator(underlyingMap, true)
   val priQ = new IntHeapPriorityQueue(comparator)

  override def recordInfo(id: Int, nodeMap: Int2IntMap) {
    throw new UnsupportedOperationException("Use recordPreviousNeighbor instead")
  }

  /**
   * Record the previous neighbor `nodeId` of `id`
   */
  def recordPreviousNeighbor(id: Int, nodeId: Int) {
    if (!(onlyOnce && infoPerNode.containsKey(id))) {
      nbrCountsPerNodeOrDefault(id).addTo(nodeId, 1)
    }
  }

  /**
   * Top previous neighborhos until node `id`
   */
  override def infoOfNode(id: Int): Option[Int2IntMap] = {
    if (underlyingMap.containsKey(id)) {
      Some(topPrevNbrsTill(id, numTopPathsPerNode))
    } else {
      None
    }
  }

  /**
   * Returns top `num` neighbors ending at `nodeId`
   * Results are sorted in decreasing order of occurrence
   */
  private def topPrevNbrsTill(nodeId: Int, num: Option[Int]): Int2IntArrayMap = {
    val result = new Int2IntArrayMap

    comparator.setNode(nodeId)
    priQ.clear()

    val infoMap = infoPerNode(nodeId)
    val nodeIterator = infoMap.keySet.iterator
    while (nodeIterator.hasNext) {
      val nbrId = nodeIterator.next()
      priQ.enqueue(nbrId)
    }

    val size = num match {
      case Some(n) => n
      case None => priQ.size
    }

    while (result.size < size && !priQ.isEmpty) {
      val nbrId = priQ.dequeueInt()
      result += ((nbrId, infoMap(nbrId)))
    }

    result
  }

  override def infoAllNodes: collection.Map[Int, Int2IntMap] = {
    val result = new Int2ObjectOpenHashMap[Int2IntMap]
    val nodeIterator = underlyingMap.keySet.iterator
    while (nodeIterator.hasNext) {
      val node = nodeIterator.nextInt
      result.put(node, topPrevNbrsTill(node, numTopPathsPerNode))
    }
    result.asInstanceOf[jutil.Map[Int, Int2IntMap]]
  }

  private def nbrCountsPerNodeOrDefault(node: Int): Int2IntOpenHashMap = {
    if (!infoPerNode.containsKey(node)) {
      underlyingMap.put(node, new Int2IntOpenHashMap)
    }
    underlyingMap.get(node)
  }
}

class PrevNbrComparator(nbrCountsPerId: Int2ObjectOpenHashMap[Int2IntOpenHashMap],
                        descending: Boolean) extends IntComparator {

  var infoMap: Int2IntOpenHashMap = null

  def setNode(id: Int) {
    infoMap = nbrCountsPerId.get(id)
  }

  // TODO ensure scala runtime does not call this boxed version
  override def compare(id1: java.lang.Integer, id2: java.lang.Integer): Int = {
    compare(id1.intValue, id2.intValue)
  }

  override def compare(id1: Int, id2: Int): Int = {
    val id1Count = infoMap.get(id1)
    val id2Count = infoMap.get(id2)

    if (id1Count != id2Count) {
      if (descending) {
        id2Count - id1Count
      } else {
        id1Count - id2Count
      }
    } else {
      id1 - id2
    }
  }
}

