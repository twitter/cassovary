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

import com.twitter.cassovary.graph.Node
import com.twitter.cassovary.util.collections.FastMap
import com.twitter.cassovary.util.collections.FastMap.FastMapFactory

/**
 * Info keeper records info per node id and returns output per node or for all nodes
 *
 * If `onlyOnce` is true keeps info only the first time a node is seen.
 */
trait InfoKeeper[@specialized(Int, Boolean) O] {
  def onlyOnce: Boolean

  protected def infoPerNode: FastMap[Int, O]

  /**
   * Record information `info` of node `id`
   */
  def recordInfo(id: Int, info: O) {
    if (!(onlyOnce && infoAllNodes.contains(id))) {
      infoPerNode.+=(id, info)
    }
  }

  /**
   * Get information of a particular node by its `id`
   */
  def infoOfNode(id: Int): Option[O] = {
    infoAllNodes.get(id)
  }

  /**
   * Get information of a particular `node`.
   */
  def infoOfNode(node: Node): Option[O] = infoOfNode(node.id)

  /**
   * Clear underlying map
   */
  def clear() {
    infoPerNode.clear()
  }

  /**
   * Get info for all nodes (immutable version visible outside)
   */
  def infoAllNodes: collection.Map[Int, O] = infoPerNode.asScala()
}

object InfoKeeper {
  def apply[O](onlyOnce: Boolean = false)(implicit fmf: FastMapFactory[Int, O, FastMap[Int, O]]):
  InfoKeeper[O] = {
    val onlyOnceRef = onlyOnce
    new InfoKeeper[O] {
      def onlyOnce: Boolean = onlyOnceRef

      val infoPerNode: FastMap[Int, O] = FastMap[Int, O]()
    }
  }
}
