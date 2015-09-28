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

import com.twitter.cassovary.util.collections
import com.twitter.cassovary.util.collections._
import collections.Implicits._

/**
 * A NodeTourist that keeps track of the previous immediate neighbor of a
 * given node in visiting sequence.
 */
class PrevNbrCounter(val numTopPathsPerNode: Option[Int], override val onlyOnce: Boolean)
    extends InfoKeeper[FastMap[Int,Int]] {

  /**
   * Keep info only the first time a node is seen
   */
  def this() = this(None, false)

  protected val prevNbrs = FastMap[Int, FastMap[Int, Int] with AddTo[Int]]()
  
  override def infoPerNode = prevNbrs.asInstanceOf[FastMap[Int, FastMap[Int, Int]]]

  /**
   * Priority queue and comparator for sorting prev nbrs. Reused across nodes.
   */
   val comparator = new PrevNbrComparator(prevNbrs, true)
   val priQ = FastQueue.priority[Int](Some(comparator))

  override def recordInfo(id: Int, nodeMap: FastMap[Int, Int]) {
    throw new UnsupportedOperationException("Use recordPreviousNeighbor instead")
  }

  /**
   * Record the previous neighbor `nodeId` of `id`
   */
  def recordPreviousNeighbor(id: Int, nodeId: Int) {
    if (!(onlyOnce && prevNbrs.contains(id))) {
      nbrCountsPerNodeOrDefault(id).addTo(nodeId, 1)
    }
  }

  /**
   * Top previous neighborhos until node `id`
   */
  override def infoOfNode(id: Int): Option[FastMap[Int, Int]] = {
    if (prevNbrs.contains(id)) {
      Some(topPrevNbrsTill(id, numTopPathsPerNode))
    } else {
      None
    }
  }

  /**
   * Returns top `num` neighbors ending at `nodeId`
   * Results are sorted in decreasing order of occurrence
   */
  private def topPrevNbrsTill(nodeId: Int, num: Option[Int]): FastMap[Int, Int] = {
    val result = FastMap[Int, Int]()

    comparator.setNode(nodeId)
    priQ.clear()

    val infoMap = prevNbrs.get(nodeId)
    val nodeIterator = infoMap.asScala().keysIterator
    while (nodeIterator.hasNext) {
      val nbrId = nodeIterator.next()
      priQ += nbrId
    }

    val size = num match {
      case Some(n) => n
      case None => priQ.size()
    }

    while (result.size < size && !priQ.isEmpty) {
      val nbrId = priQ.deque()
      result += (nbrId, infoMap.get(nbrId))
    }

    result
  }

  override def infoAllNodes: collection.Map[Int, FastMap[Int, Int]] = {
    val result = FastMap[Int, FastMap[Int, Int]]()
    val nodeIterator = prevNbrs.asScala().keysIterator
    while (nodeIterator.hasNext) {
      val node = nodeIterator.next()
      result += (node, topPrevNbrsTill(node, numTopPathsPerNode))
    }
    result.asScala()
  }

  private def nbrCountsPerNodeOrDefault(node: Int): FastMap[Int, Int] with AddTo[Int] = {
    if (!prevNbrs.contains(node)) {
      prevNbrs += (node, FastMap.applyFor[Int, Int, FastMap[Int, Int] with AddTo[Int]]())
    }
    prevNbrs.get(node)
  }

}

class PrevNbrComparator(nbrCountsPerId: FastMap[Int, FastMap[Int, Int] with AddTo[Int]],
                        descending: Boolean) extends Order[Int] {

  var infoMap: FastMap[Int, Int] = null

  def setNode(id: Int) {
    infoMap = nbrCountsPerId.get(id)
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
