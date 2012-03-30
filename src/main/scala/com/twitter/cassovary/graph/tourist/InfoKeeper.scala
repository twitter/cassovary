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

import com.twitter.cassovary.graph.Node
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

/**
 * An InfoKeeper keeps caller-supplied info per node.
 * It provides another method recordInfo() to record a given info for a node.
 */
trait InfoKeeper[O] {
  /**
   * Keep info only the first time a node is seen
   */
  val onlyOnce = false

  protected val infoPerNode = new Int2ObjectOpenHashMap[O]

  /**
   * Record information {@code info} of node {@code id}.
   */
  def recordInfo(id: Int, info: O) {
    if (!(onlyOnce && infoPerNode.containsKey(id))) {
      infoPerNode.put(id, info)
    }
  }

  /**
   * Get information of a particular node by its {@code id}
   */
  def infoOfNode(id: Int): Option[O] = {
    if (infoPerNode.containsKey(id)) {
      Some(infoPerNode.get(id))
    } else {
      None
    }
  }

  /**
   * Get information of a particular {@code node}.
   */
  def infoOfNode(node: Node): Option[O] = infoOfNode(node.id)

  /**
   * Clear underlying map
   */
  def clear() {
    infoPerNode.clear()
  }

  /**
   * Get info for all nodes as an array of (key, value) tuples
   */
  def infoAllNodes: Array[(Int, O)] = {
    val allPairs = new Array[(Int, O)](infoPerNode.size)

    var counter = 0
    val nodeIterator = allPairs.keySet.iterator
    while (nodeIterator.hasNext) {
      val node = nodeIterator.next
      allPairs(counter) = (node, infoPerNode.get(node))
      counter += 1
    }

    allPairs
  }
}