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

/**
 * Info keeper records info per node and returns output per node or for all nodes
 */
trait InfoKeeper[@specialized(Int) I, @specialized(Int) O, M] {
  /**
   * Keep info only the first time a node is seen
   */
  val onlyOnce = false

  /**
   * Record information {@code info} of node {@code id}
   */
  def recordInfo(id: Int, info: I)

  /**
   * Get information of a paticular node by its {@code id}
   */
  def infoOfNode(id: Int): Option[O]

  /**
   * Get information of a particular {@code node}.
   */
  def infoOfNode(node: Node): Option[O] = infoOfNode(node.id)

  /**
   * Clear underlying map
   */
  def clear()

  /**
   * Get info for all nodes
   */
  def infoAllNodes: M
}
