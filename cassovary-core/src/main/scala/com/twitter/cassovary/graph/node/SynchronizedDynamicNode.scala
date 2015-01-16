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
package com.twitter.cassovary.graph.node

import scala.collection.mutable.ArrayBuffer
import SynchronizedDynamicNode.DELETED_MARKER

/**
 * A Node supports add and delete operation on its in/out edges.
 * All its operations are synchronized.
 */
class SynchronizedDynamicNode(val id: Int) extends DynamicNode {
  protected val inEdges = new ArrayBuffer[Int]
  protected val outEdges = new ArrayBuffer[Int]
  require(id != DELETED_MARKER,
    "ID " + DELETED_MARKER + " is not allowed.")

  /**
   * Override functions of base class.
   */
  def inboundNodes = {
    val array = synchronized { inEdges.toArray }
    array filter { _ != DELETED_MARKER}
  }
  def outboundNodes = synchronized {
    val array = synchronized { outEdges.toArray }
    array filter {_ != DELETED_MARKER}
  }

  override def equals(other: Any) = {
    other match {
      case node: SynchronizedDynamicNode => id == node.id
      case _ => false
    }
  }

  /**
   * Add multiple inbound edges {@code nodeIds}.
   * It doesn't examine whether some ids is already in.
   */
  def addInBoundNodes(nodeIds: Seq[Int]) {
    synchronized {
      nodeIds.foreach { id => inEdges += id }
    }
  }

  /**
   * Add multiple outbound edges {@code nodeIds}. Ignore the edges that are already in.
   * It doesn't examine whether the some ids in {@code nodeIds} is already in.
   */
  def addOutBoundNodes(nodeIds: Seq[Int]) {
    synchronized {
      nodeIds.foreach { id => outEdges += id }
    }
  }

  /**
   * Remove an inbound edge {@code nodeId}.
   */
  def removeInBoundNode(nodeId: Int) = markDelete(inEdges, nodeId)

  /**
   * Remove an outbound edge {@code nodeId}.
   */
  def removeOutBoundNode(nodeId: Int) = markDelete(outEdges, nodeId)

  private def markDelete(edges: ArrayBuffer[Int], nodeId: Int) = synchronized {
    // TODO(taotao): reclaim deleted slots.
    val pos = edges.indexOf(nodeId)
    if (pos >= 0) {
      edges(pos) = DELETED_MARKER
    }
    pos
  }

  override def toString = {
    val inNodes = synchronized { inboundNodes.toList }
    val outNodes = synchronized { outboundNodes.toList }
    "SynchronizedDynamicNode(id=%d, out=%s, in=%s)".format(id, outNodes, inNodes)
  }
}

object SynchronizedDynamicNode {
  val DELETED_MARKER = Integer.MIN_VALUE
}
