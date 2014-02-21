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

import com.twitter.cassovary.graph.Node

/**
 * Represents a dynamic node in a directed graph.
 * DynamicNode can add and delete its in and out edges dynamically.
 */
trait DynamicNode extends Node {
  /**
   * Add single inbound edge {@code nodeId} into the inbound list
   */
  def addInBoundNode(nodeId: Int) = addInBoundNodes(Seq(nodeId))

  /**
   * Add inbound edges {@code nodeIds} into the inbound list.
   */
  def addInBoundNodes(nodeIds: Seq[Int]): Unit

  /**
   * Add single outbound edge {@code nodeId} into the outbound list
   */
  def addOutBoundNode(nodeId: Int) = addOutBoundNodes(Seq(nodeId))


  /**
   * Add outbound edges {@code nodeId} into the outbound list
   */
  def addOutBoundNodes(nodeIds: Seq[Int]): Unit

  /**
   * Remove a single edge {@code nodeId} from the inbound list.
   */
  def removeInBoundNode(nodeId: Int): Unit

  /**
   * Remove a single edge {@code nodeId} from the outbound list.
   */
  def removeOutBoundNode(nodeId: Int): Unit
}
