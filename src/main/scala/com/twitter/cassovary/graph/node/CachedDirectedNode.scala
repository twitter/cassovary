package com.twitter.cassovary.graph.node

import com.twitter.cassovary.graph.{StoredGraphDir, Node}
import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir
import com.twitter.cassovary.util.IntArrayCache

abstract class CachedDirectedNode (val id: Int) extends Node

object CachedDirectedNode {

  def apply(nodeId: Int, neighborSize: Int, dir: StoredGraphDir, cache: IntArrayCache) = {
    dir match {
      case StoredGraphDir.OnlyIn =>
        new UniDirectionalNode(nodeId) {
          def inboundNodes = cache.get(nodeId)
          def outboundNodes = Nil
          override def inboundCount = neighborSize
          override def outboundCount = 0
        }
      case StoredGraphDir.OnlyOut =>
        new UniDirectionalNode(nodeId) {
          def inboundNodes = Nil
          def outboundNodes = cache.get(nodeId)
          override def inboundCount = 0
          override def outboundCount = neighborSize
        }
      case StoredGraphDir.Mutual =>
        new UniDirectionalNode(nodeId) {
          def inboundNodes = cache.get(nodeId)
          def outboundNodes = cache.get(nodeId)
          override def inboundCount = neighborSize
          override def outboundCount = neighborSize
        }
    }
  }
}
