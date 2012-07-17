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
import java.io.File
import net.lag.logging.Logger
import com.twitter.ostrich.stats.Stats

/**
 * Wrapper around a graph to simulate a cache
 * @param g The backing graph (the actual graph)
 * @param cacheSize size of the cache you want
 * @param cacheMechanism what cache mechanism to use ("lru", etc.)
 * @param statsInterval how often stats should be logged
 * @param outputDirectory which directory stats should be logged to
 */
class GraphWithSimulatedCache(val g: DirectedGraph, val cacheSize: Int,
                              val cacheMechanism: String, val statsInterval: Long,
                              val outputDirectory: String) extends DirectedGraph {
  
  protected val log = Logger.get
  var writes = 0

  val cache:SimulatedCache = cacheMechanism match {
    case "clock" => {
      val dg = g.asInstanceOf[DirectedGraph]
      new ClockSimulatedCache(dg.maxNodeId, cacheSize)
    }
    case "mru" => new MRUSimulatedCache(cacheSize)
    case _ => {
      val dg = g.asInstanceOf[DirectedGraph]
      new FastLRUSimulatedCache(dg.maxNodeId, cacheSize)
    }
  }

  override def getNodeById(id: Int) = {
    val node = g.getNodeById(id)
    node match {
      case Some(n) => {
        cache.getAndUpdate(id) // TODO make sure id is valid!
        if (cache.accesses % statsInterval == 0) {
          val (m, a, r) = cache.getStats
          Stats.addMetric("cache_misses", m.toInt)
          Stats.addMetric("cache_accesses", a.toInt)
          log.info("simcache interval %s %s %s".format(m, a, r))
          writeStats("%s/%s.txt".format(outputDirectory, writes))
          writes += 1
        }
      }
      case _ => ()
    }
    node
  }

  override def existsNodeId(id: Int) = g.existsNodeId(id)

  def graph = g

  def getStats = cache.getStats

  def diffStat = cache.diffStat

  def writeStats(fileName: String) = {
    val (misses, accesses, missRatio) = cache.getStats
    printToFile(new File(fileName))(p => {
      p.println("%s\t%s\t%s".format(misses, accesses, missRatio))
    })
  }

  private def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def nodeCount = g.nodeCount
  val storedGraphDir = g.storedGraphDir
  def edgeCount = g.edgeCount
  def iterator = g.iterator
}

/**
 * Wrapper around a graph to simulate a cache. Similar to GraphWithSimulatedCache
 * except that this takes into account the cost of storing graph edges, rather than
 * treating each node as costing a single unit
 * @param g The backing graph (the actual graph)
 * @param cacheSize size of the cache you want
 * @param cacheMechanism what cache mechanism to use ("lru", etc.)
 * @param statsInterval how often stats should be logged
 * @param outputDirectory which directory stats should be logged to
 */
class GraphWithSimulatedVarCache(g: DirectedGraph, cacheSize: Int,
    cacheMechanism: String, statsInterval: Long, outputDirectory: String)
  extends GraphWithSimulatedCache(g, cacheSize, cacheMechanism,
    statsInterval, outputDirectory) {

  override def getNodeById(id: Int) = {
    val node = g.getNodeById(id)
    node match {
      case Some(n) => {
        cache.getAndUpdate(id, n.neighborCount(GraphDir.OutDir))
        if (cache.accesses % statsInterval == 0) {
          val (m, a, r) = cache.getStats
          Stats.addMetric("cache_misses", m.toInt)
          Stats.addMetric("cache_accesses", a.toInt)
          log.info("simcache interval %s %s %s".format(m, a, r))
          writeStats("%s/%s.txt".format(outputDirectory, writes))
          writes += 1
        }
      }
      case _ => ()
    }
    node
  }
}