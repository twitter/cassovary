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
package com.twitter.cassovary.util

import com.twitter.cassovary.graph.{DirectedGraph, GraphDir}
import net.lag.logging.Logger

/**
 * Wrapper around a graph to simulate a cache
 * Note here that unlike the real CachedDirectedGraph, nodes are "retrieved" from the cache once
 * getNodeById is called. In CachedDirectedGraph, the cache is accessed only when retrieving neighbor ids.
 * @param graph The backing graph (the actual graph)
 * @param cache The cache to use
 */
class GraphWithSimulatedCache(val graph: DirectedGraph, val cache: SimulatedCache) extends DirectedGraph {
  
  protected val log = Logger.get("GraphWithSimulatedCache")
  var writes = 0

  override def getNodeById(id: Int) = {
    val node = graph.getNodeById(id)
    node match {
      case Some(n) => {
        cache.get(id) // TODO make sure id is valid!
      }
      case _ => None
    }
    node
  }

  override def existsNodeId(id: Int) = graph.existsNodeId(id)

  def getStats = cache.getStats

  def numAccessesAtFirstMiss = cache.numAccessesAtFirstMiss

  def diffStat = cache.diffStat

  def outputDirectory = cache.outputDirectory

  def nodeCount = graph.nodeCount
  val storedGraphDir = graph.storedGraphDir
  def edgeCount = graph.edgeCount
  def iterator = graph.iterator
}

/**
 * Wrapper around a graph to simulate a cache. Similar to GraphWithSimulatedCache
 * except that this takes into account the cost of storing graph edges, rather than
 * treating each node as costing a single unit
 * @param graph The backing graph (the actual graph)
 * @param cache The cache to use
 */
class GraphWithSimulatedVarCache(graph: DirectedGraph, cache: SimulatedCache)
  extends GraphWithSimulatedCache(graph, cache) {

  override def getNodeById(id: Int) = {
    val node = graph.getNodeById(id)
    node match {
      case Some(n) => {
        cache.get(id, n.neighborCount(GraphDir.OutDir))
      }
      case _ => None
    }
    node
  }
}
