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

import com.twitter.cassovary.util.collections._

import com.twitter.cassovary.util.collections.Implicits._

/**
 * A tourist that keeps counts of the number of times a node has been seen.
 */
class VisitsCounter extends InfoKeeper[Int] with NodeTourist {
  override def onlyOnce: Boolean = false

  private val countersMap = FastMap.applyFor[Int, Int, FastMap[Int, Int] with AddTo[Int]]()

  /**
   * PriorityQueue and IntComparator for sorting
   */
  val comparator = new VisitsComparator(countersMap, true)
  val priQ = FastQueue.priority[Int](Some(comparator))

  def visit(id: Int) {
    countersMap.addTo(id, 1)
  }

  /**
   * Returns a map of nodes to visit count, sorted in decreasing order when iterating
   */
  protected override def infoPerNode = {
    val resultMap = FastMap.applyFor[Int, Int, FastMap[Int, Int] with InsertionOrderIterator]()

    priQ.clear()

    val nodeIterator = countersMap.asScala().keysIterator
    while (nodeIterator.hasNext) {
      val node = nodeIterator.next()
      priQ += node
    }

    while (!priQ.isEmpty) {
      val node = priQ.deque()
      resultMap += (node, countersMap.get(node))
    }

    resultMap
  }
}

class VisitsComparator(infoPerNode: FastMap[Int, Int], descending: Boolean) extends Order[Int] {
  override def compare(id1: Int, id2: Int): Int = {
    val id1Count = infoPerNode.get(id1)
    val id2Count = infoPerNode.get(id2)

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
