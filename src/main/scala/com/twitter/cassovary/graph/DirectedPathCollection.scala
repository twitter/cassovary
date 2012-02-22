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

import collection.mutable


/**
 * Represents a collection of directed paths. For example, a collection of paths out of
 * the same source node.
 */
class DirectedPathCollection[T] {

  // the current path being built
  private val currPath = DirectedPath.builder[T]()

  // pathCountsPerId(id)(path) = count of #times path has been seen ending in id
  private val pathCountsPerId = new mutable.HashMap[T, mutable.HashMap[DirectedPath[T], Int]] {
    override def default(node: T) = {
      val h1 = new mutable.HashMap[DirectedPath[T], Int] {
        override def default(p: DirectedPath[T]) = 0
      }
      put(node, h1)
      h1
    }
  }

  /**
   * Appends node to the current path and record this path against the node.
   * @param node the node being visited
   */
  def appendToCurrentPath(node: T) {
    currPath.append(node)
    pathCountsPerId(node)(currPath.snapshot) += 1
  }

  /**
   * Reset the current path
   */
  def resetCurrentPath() {
    currPath.clear()
  }

  /**
   * @return a list of tuple(path, count) where count is the number of times path
   *         ending at {@code node} is observed
   */
  def topPathsTill(node: T, num: Int) = {
    val paths = pathCountsPerId(node)
    val top = paths.toList.sortBy { case (path, count) => (-count, path.length) }
    top.take(num)
  }

  /**
   * @param num the number of top paths to return for a node
   * @return a mapping of node to the list of top paths ending at node
   */
  def topPathsPerNodeId(num: Int): collection.Map[T, List[(DirectedPath[T], Int)]] = {
    Map.empty ++ pathCountsPerId.keysIterator.map { node =>
        (node, topPathsTill(node, num))
    }
  }

  /**
   * @return the total number of unique paths that end at {@code node}
   */
  def numUniquePathsTill(node: T) = pathCountsPerId(node).size

  /**
   * @return the total number of unique paths in this collection
   */
  def totalNumPaths = {
    pathCountsPerId.keysIterator.foldLeft(0) { case (tot, node) => tot + numUniquePathsTill(node) }
  }

}
