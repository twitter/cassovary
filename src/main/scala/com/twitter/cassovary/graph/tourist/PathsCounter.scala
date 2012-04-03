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
package com.twitter.cassovary.graph.tourist

import com.twitter.cassovary.graph.{DirectedPath, DirectedPathCollection}
import it.unimi.dsi.fastutil.ints.Int2ObjectMap
import it.unimi.dsi.fastutil.objects.Object2IntMap

/**
 * A tourist that keeps track of the paths ending at each node. It keeps
 * at {@code numTopPathsPerNode} paths per node.
 *
 * TODO: instead of homeNodeIds, this func should take
 * a function param of Node => Boolean
 */

class PathsCounter(numTopPathsPerNode: Int, homeNodeIds: Seq[Int])
    extends NodeTourist with InfoKeeper[Int, Object2IntMap[DirectedPath],
      Int2ObjectMap[Object2IntMap[DirectedPath]]] {

  def this() = this(0, Nil)

  private val paths = new DirectedPathCollection

  def visit(id: Int) {
    if (homeNodeIds.contains(id)) {
      paths.resetCurrentPath()
    }
    paths.appendToCurrentPath(id)
  }

  def recordInfo(id: Int, info: Int) {
    // NOOP use visit
  }

  def infoOfNode(id: Int): Option[Object2IntMap[DirectedPath]] = {
    if (paths.containsNode(id)) {
      Some(paths.topPathsTill(id, numTopPathsPerNode))
    } else {
      None
    }
  }

  def infoAllNodes: Int2ObjectMap[Object2IntMap[DirectedPath]] = paths.topPathsPerNodeId(numTopPathsPerNode)

  def clear() {
    paths.clear()
  }
}
