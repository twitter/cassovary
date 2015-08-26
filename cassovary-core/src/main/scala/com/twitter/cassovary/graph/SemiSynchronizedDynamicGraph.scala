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

import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.graph.node.DynamicNode

/**
 * An efficient dynamic graph implementation which supports concurrent reading and writing, as long as only a single
 * thread is writing at a time.  This restriction allows it to avoid the use of locks or synchronized sections of code
 * apart from the locking the occurs when nodes are added to a ConcurrentHashMap.
 * Nodes are stored in a ConcurrentHashMap, and neighbors of each node are stored in Array[Int]s.  Currently, only edge
 * and node addition (not deletion) is supported.
 */
class SemiSynchronizedDynamicGraph()
    extends DynamicDirectedGraphHashMap(BothInOut) {
  override def nodeFactory(id: Int): DynamicNode = new SemiSynchronizedNode(id)
}

private class SemiSynchronizedNode(val id: Int) extends DynamicNode {
  val outboundList = new SemiSynchronizedIntArrayList()
  val inboundList = new SemiSynchronizedIntArrayList()

  override def outboundNodes(): Seq[Int] = outboundList.toSeq
  override def inboundNodes(): Seq[Int] = inboundList.toSeq
  /**
   * Add outbound edges {@code nodeId} into the outbound list
   */
  override def addOutBoundNodes(nodeIds: Seq[Int]): Unit =
    outboundList.append(nodeIds)

  /**
   * Add inbound edges {@code nodeIds} into the inbound list.
   */
  override def addInBoundNodes(nodeIds: Seq[Int]): Unit =
    inboundList.append(nodeIds)

  override def removeInBoundNode(nodeId: Int): Unit = throw new UnsupportedOperationException()
  override def removeOutBoundNode(nodeId: Int): Unit = throw new UnsupportedOperationException()
}

/** A resizable array of Ints with limited functionality.  It supports concurrent reading and writing as long as only
  * one thread writes at a time.  The only mutable operation supported is appending Ints (not changing or removing current Ints).
  */
// We store Ints in an array padded with extra capacity that will grow over time
// We essentially want a fastutil IntArrayList, but to get volatile references we will manage the array resizing
// explicitly.
private class SemiSynchronizedIntArrayList {
  @volatile private var intArray: Array[Int] = new Array[Int](SemiSynchronizedIntArrayList.initialCapacity)
  @volatile private var size = 0

  def append(ints: Seq[Int]): Unit = {
    if (size + ints.size > intArray.length) {
      val newCapacity = math.max(
        (intArray.length * SemiSynchronizedIntArrayList.resizeFactor).toInt,
        size + ints.size)
      val newIntArray = new Array[Int](newCapacity)
      System.arraycopy(intArray, 0, newIntArray, 0, size)
      intArray = newIntArray
    }
    // Update outgoingArray before updating size, so concurrent reader threads don't read past the end of the array
    for (i <- 0 until ints.size) {
      intArray(i + size) = ints(i)
    }
    size = size + ints.size
  }

  /** Returns an immutable view of the current Ints in this object.
   */
  def toSeq: IndexedSeq[Int] = new IntArrayView(size, intArray)
}

object SemiSynchronizedIntArrayList {
  val initialCapacity = 2
  val resizeFactor = 2.0
}

/**
 * Stores a reference to an array and a size.  The view is immutable assuming the first size entries of the array don't change.
 */
class IntArrayView(override val size: Int, private val intArray: Array[Int]) extends IndexedSeq[Int] {
  override def length: Int = size
  override def apply(idx: Int): Int = intArray(idx)
}
