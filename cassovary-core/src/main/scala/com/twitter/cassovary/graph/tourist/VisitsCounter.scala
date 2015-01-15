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
import java.util
import scala.collection.JavaConversions._

/**
 * A tourist that keeps counts of the number of times a node has been seen.
 */
class VisitsCounter extends IntInfoKeeper(false) with NodeTourist {

  /**
   * PriorityQueue and IntComparator for sorting
   */
  val comparator = new VisitsComparator(underlyingMap, true)
  val priQ = new IntHeapPriorityQueue(comparator)

  def visit(id: Int) {
    underlyingMap.add(id, 1)
  }

  /**
   * Returns a map of nodes to visit count, sorted in decreasing order when iterating
   */
  protected override def infoPerNode = {
    val resultMap = new Int2IntArrayMap

    priQ.clear()

    val nodeIterator = underlyingMap.keySet.iterator
    while (nodeIterator.hasNext) {
      val node = nodeIterator.next()
      priQ.enqueue(node)
    }

    while (!priQ.isEmpty) {
      val node = priQ.dequeueInt()
      resultMap.put(node, underlyingMap.get(node))
    }

    resultMap.asInstanceOf[util.Map[Int, Int]]
  }
}

class VisitsComparator(infoPerNode: Int2IntMap, descending: Boolean) extends IntComparator {
  // TODO ensure scala runtime does not call this boxed version
  override def compare(id1: java.lang.Integer, id2: java.lang.Integer): Int = {
    compare(id1.intValue, id2.intValue)
  }

  override def compare(id1: Int, id2: Int): Int = {
    val id1Count = infoPerNode(id1)
    val id2Count = infoPerNode(id2)

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