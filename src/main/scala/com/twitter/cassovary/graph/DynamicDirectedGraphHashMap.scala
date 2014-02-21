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

import com.twitter.cassovary.graph.node.DynamicNode
import java.util.concurrent.ConcurrentHashMap
import StoredGraphDir._

/**
 * A class supporting dynamic addition of new nodes and addition/deletion of edges in
 * existing nodes. It currently doesn't support deletion of nodes.
 */
abstract class DynamicDirectedGraphHashMap(val storedGraphDir: StoredGraphDir)
    extends DynamicDirectedGraph {

  protected def nodeFactory(id: Int): DynamicNode

  val nodes = new ConcurrentHashMap[Int, DynamicNode]

  def iterator = {
    val elements = nodes.elements
    new Iterator[Node] {
       def hasNext = elements.hasMoreElements
       def next = elements.nextElement.asInstanceOf[Node]
       def remove = throw new UnsupportedOperationException()
    }
  }

  /**
   * Get the total number of nodes in the graph.
   */
  def nodeCount = nodes.size

  /**
   * Get the total number of edges in the graph. It's a very slow function. Use it carefully.
   */
  def edgeCount = iterator.foldLeft(0) {(accum, node) =>
    accum + node.inboundCount  + node.outboundCount
  }

  /**
   * Get a node given a nodeId {@code id}. Returns None if the id is in in the graph.
   */
  def getNodeById(id: Int): Option[DynamicNode] = {
    val n = nodes.get(id)
    if (n == null) None
    else Some(n)
  }

  /**
   * Get or create a node given a nodeId {@code id} and return it.
   */
  def getOrCreateNode(id: Int): DynamicNode = {
    val n = nodes.get(id)
    if (n != null) {
      n
    } else {
      // n does not exist (as far as this thread is concerned)
      val newn = nodeFactory(id)
      // try putting this in nodes. Might lose out to some other thread
      val existing = nodes.putIfAbsent(id, newn)
      if (existing == null) { /* this thread won */
          newn
      } else { /* this thread lost to some other thread */
        nodes.get(id)
      }
    }
  }

  /**
   * Don't support this operator. It always throws exceptions.
   */
  override lazy val maxNodeId = {
    throw new Exception("DynamicGraph doesn't support maxNodeId")
  }

  def addEdge(srcId: Int, destId: Int) {
    val srcNode = getOrCreateNode(srcId)
    val destNode = getOrCreateNode(destId)

    storedGraphDir match {
      case OnlyOut => srcNode.addOutBoundNode(destId)
      case OnlyIn => destNode.addInBoundNode(srcId)
      case BothInOut => {
        srcNode.addOutBoundNode(destId)
        destNode.addInBoundNode(srcId)
      }
    }
  }

  /**
   * Remove an edge {@code edgeId) from a node {@code id}. {@code isOut} indicates
   * whether it's in edge (Some(false)), out edge (Some(true)), or both (None).
   */
  def removeEdge(srcId: Int, destId: Int) = {
    val srcNode = getNodeById(srcId)
    val destNode = getNodeById(destId)

    if (srcNode.isDefined && destNode.isDefined) {
      val sn = srcNode.get
      val dn = destNode.get

      storedGraphDir match {
        case OnlyOut => sn.removeOutBoundNode(destId)
        case OnlyIn => dn.removeInBoundNode(srcId)
        case BothInOut => {
          sn.removeOutBoundNode(destId)
          dn.removeInBoundNode(srcId)
        }
      }
    }

    (srcNode, destNode)
  }
}
