package com.twitter.cassovary.graph.node

import com.twitter.cassovary.graph.{StoredGraphDir, Node}
import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir
import com.twitter.cassovary.util.IntArrayCache

abstract class CachedDirectedNode (var id: Int, var size: Int) extends Node

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