/*
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

import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.graph.node.DynamicNode

/**
 * An efficient dynamic graph implementation which supports concurrent reading and writing.
 * Reads happen without locks or data copying.
 * Nodes are stored in a ConcurrentHashMap, and neighbors of each node are stored in Array[Int]s.
 * Currently, only edge
 * and node addition (not deletion) is supported.
 */
class ConcurrentHashMapDynamicGraph(storedGraphDir: StoredGraphDir = BothInOut)
    extends DynamicDirectedGraphHashMap(storedGraphDir) {
  def nodeFactory(id: Int): DynamicNode = new ConcurrentNode(id)
  override def removeEdge(srcId: Int, destId: Int) = throw new UnsupportedOperationException()
}

private class ConcurrentNode(val id: Int) extends DynamicNode {
  val outboundList = new ConcurrentIntArrayList()
  val inboundList = new ConcurrentIntArrayList()

  def outboundNodes(): Seq[Int] = outboundList
  def inboundNodes(): Seq[Int] = inboundList
  /**
   * Add outbound edges {@code nodeIds} into the outbound list.
   */
  def addOutBoundNodes(nodeIds: Seq[Int]): Unit =
    outboundList.append(nodeIds)

  /**
   * Add inbound edges {@code nodeIds} into the inbound list.
   */
  def addInBoundNodes(nodeIds: Seq[Int]): Unit =
    inboundList.append(nodeIds)

  def removeInBoundNode(nodeId: Int): Unit = throw new UnsupportedOperationException()
  def removeOutBoundNode(nodeId: Int): Unit = throw new UnsupportedOperationException()
}

/** A resizable sequence of Ints which supports appending Ints (but not changing or removing
  * current Ints).
  * It supports concurrent reading and writing.
  */
// We store Ints in an array padded with extra capacity that will grow over time.
// We essentially want a fastutil IntArrayList, but with synchronized appends.
private class ConcurrentIntArrayList extends collection.IndexedSeq[Int] {
  @volatile private var intArray: Array[Int] = new Array[Int](ConcurrentIntArrayList.initialCapacity)
  @volatile private var _size = 0

  def append(ints: Seq[Int]): Unit = {
    this.synchronized {
      if (_size + ints.size > intArray.length) {
        val newCapacity = math.max(
          (intArray.length * ConcurrentIntArrayList.resizeFactor).toInt,
          _size + ints.size)
        val newIntArray = new Array[Int](newCapacity)
        System.arraycopy(intArray, 0, newIntArray, 0, _size)
        intArray = newIntArray
      }
      // Update outgoingArray before updating size, so concurrent reader threads don't read past the
      // end of the array
      for (i <- 0 until ints.size) {
        intArray(i + _size) = ints(i)
      }
      _size = _size + ints.size
    }
  }

  def length: Int = _size

  def apply(idx: Int): Int = intArray(idx)
}

object ConcurrentIntArrayList {
  val initialCapacity = 2
  val resizeFactor = 2.0
}
