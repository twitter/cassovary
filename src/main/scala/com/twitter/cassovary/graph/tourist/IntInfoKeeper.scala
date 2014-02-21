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
import it.unimi.dsi.fastutil.ints.{Int2IntMap, Int2IntOpenHashMap}

/**
 * An InfoKeeper keeps caller-supplied info per node.
 * It provides another method recordInfo() to record a given info for a node.
 */
class IntInfoKeeper(override val onlyOnce: Boolean) extends InfoKeeper[Int, Int, Int2IntMap] {

  protected val infoPerNode = new Int2IntOpenHashMap

  /**
   * Record information {@code info} of node {@code id}.
   */
  def recordInfo(id: Int, info: Int) {
    if (!(onlyOnce && infoPerNode.containsKey(id))) {
      infoPerNode.put(id, info)
    }
  }

  def incrementInfo(id: Int, increment: Int) {
    if (!(onlyOnce && infoPerNode.containsKey(id))) {
      infoPerNode.add(id, increment)
    }
  }

  /**
   * Get information of a particular node by its {@code id}
   */
  def infoOfNode(id: Int): Option[Int] = {
    if (infoPerNode.containsKey(id)) {
      Some(infoPerNode.get(id))
    } else {
      None
    }
  }

  /**
   * Clear underlying map
   */
  def clear() {
    infoPerNode.clear()
  }

  /**
   * Get info for all nodes as an array of (key, value) tuples
   */
  def infoAllNodes: Int2IntMap = infoPerNode
}