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

/**
 * The entry point into a model of a graph.  Users typically query a known starting node
 * and then traverse the graph using methods on that {@code Node}.
 */
trait Graph[+V <: Node] {
  /**
   * Returns the node with the given {@code id} or else {@code None} if the given node does not
   * exist in this graph.
   */
  def getNodeById(id: Int): Option[V]

  def existsNodeId(id: Int) = getNodeById(id).isDefined
}
