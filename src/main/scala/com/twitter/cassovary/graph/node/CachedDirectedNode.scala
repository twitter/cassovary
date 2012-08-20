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
package com.twitter.cassovary.graph.node

import com.twitter.cassovary.graph.{StoredGraphDir, Node}
import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir
import com.twitter.cassovary.util.cache.IntArrayCache

abstract class CachedDirectedNode (var id: Int, var size: Int) extends Node

/**
 * Version of a node used by CacheDirectedGraph which references a cache object so that
 * the node can fetch its neighbor array from the cache only when the array is needed.
 */
object CachedDirectedNode {

  def apply(nodeId: Int, neighborSize: Int, dir: StoredGraphDir, cache: IntArrayCache) = {
    dir match {
      case StoredGraphDir.OnlyIn =>
        new CachedDirectedNode(nodeId, neighborSize) {
          def inboundNodes = cache.get(id)
          def outboundNodes = Nil
          override def inboundCount = size
          override def outboundCount = 0
        }
      case StoredGraphDir.OnlyOut =>
        new CachedDirectedNode(nodeId, neighborSize) {
          def inboundNodes = Nil
          def outboundNodes = cache.get(id)
          override def inboundCount = 0
          override def outboundCount = size
        }
      case StoredGraphDir.Mutual =>
        new CachedDirectedNode(nodeId, neighborSize) {
          def inboundNodes = cache.get(id)
          def outboundNodes = cache.get(id)
          override def inboundCount = size
          override def outboundCount = size
        }
    }
  }
}

object EmptyDirectedNode {
  val empty = Array.empty[Int]

  def apply(nodeId: Int, dir: StoredGraphDir) = {
    dir match {
      case StoredGraphDir.OnlyIn =>
        new CachedDirectedNode(nodeId, 0) {
          def inboundNodes = empty
          def outboundNodes = Nil
          override def inboundCount = size
          override def outboundCount = 0
        }
      case StoredGraphDir.OnlyOut =>
        new CachedDirectedNode(nodeId, 0) {
          def inboundNodes = Nil
          def outboundNodes = empty
          override def inboundCount = 0
          override def outboundCount = size
        }
      case StoredGraphDir.Mutual =>
        new CachedDirectedNode(nodeId, 0) {
          def inboundNodes = empty
          def outboundNodes = empty
          override def inboundCount = size
          override def outboundCount = size
        }
    }
  }
}