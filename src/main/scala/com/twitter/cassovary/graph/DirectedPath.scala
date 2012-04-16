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
package com.twitter.cassovary.graph

import it.unimi.dsi.fastutil.ints.IntArrayList
import java.util.Arrays

/**
 * Represents a directed path of nodes in a graph. No loop detection is done.
 */
case class DirectedPath(val nodes: Array[Int]) {

  /**
   * @return the number of nodes on this path
   */
  def length = nodes.length

  /**
   * Check if this path contains a given node.
   * @param node the node to check membership of in this path
   * @return <code>true</code> if the node is in this path
   */
  def exists(node: Int) = nodes.contains(node)

  override def equals(other: Any): Boolean = {
    if (other.isInstanceOf[DirectedPath]) {
      Arrays.equals(nodes, other.asInstanceOf[DirectedPath].nodes)
    } else {
      false
    }
  }

  override def hashCode(): Int = {
    Arrays.hashCode(nodes)
  }
}

object DirectedPath {
  trait Builder {
    /**
     * Appends a node to the end of this path.
     * @param node the node to append
     * @return this Builder for chaining
     */
    def append(node: Int): this.type

    /**
     * Takes the snapshot of the path currently being built to return an immutable DirectedPath.
     */
    def snapshot: DirectedPath

    /**
     * Clear this path
     */
    def clear()
  }

  def builder(): Builder = {
    new Builder() {
      private val path = new IntArrayList

      def append(node: Int) = {
        path.add(node)
        this
      }

      def snapshot = {
        val intArray = new Array[Int](path.size)
        path.toArray(intArray)
        DirectedPath(intArray)
      }

      def clear() { path.clear() }
    }
  }

}
