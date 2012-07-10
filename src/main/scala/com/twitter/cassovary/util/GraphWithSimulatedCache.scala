package com.twitter.cassovary.util

import com.twitter.cassovary.graph.{DirectedGraph, Graph}
import java.io.File
import net.lag.logging.Logger
import com.twitter.ostrich.stats.Stats
import com.twitter.cassovary.graph.GraphDir

class GraphWithSimulatedCache(val g: Graph, val cacheSize: Int, val cacheMechanism: String, val statsInterval: Int) extends Graph {
  
  protected val log = Logger.get
  
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
    cache.get(id) // TODO make sure id is valid!
    if (cache.accesses % statsInterval == 0) {
      val (m, a, r) = cache.getStats
      Stats.addMetric("cache_misses", m.toInt)
      Stats.addMetric("cache_accesses", a.toInt)
      log.info("simcache interval %s %s %s".format(m, a, r))
    }
    
    g.getNodeById(id)
  }

  override def existsNodeId(id: Int) = {
    g.existsNodeId(id)
  }

  def getStats = {
    cache.getStats
  }
  
  def diffStat = {
    cache.diffStat
  }

  def writeStats(fileName: String) = {
    val (misses, accesses, missRatio) = cache.getStats
    printToFile(new File(fileName))(p => {
      p.println("%s\t%s\t%s".format(misses, accesses, missRatio))
    })
  }

  def graph = {
    g
  }

  private def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
}

class GraphWithSimulatedVarCache(g: Graph, cacheSize: Int, cacheMechanism: String, statsInterval: Int)
  extends GraphWithSimulatedCache(g, cacheSize, cacheMechanism, statsInterval) {
  override def getNodeById(id: Int) = {
    val node = g.getNodeById(id)
    node match {
      case Some(n) => {
        cache.get(id, n.neighborCount(GraphDir.OutDir))
        if (cache.accesses % statsInterval == 0) {
          val (m, a, r) = cache.getStats
          Stats.addMetric("cache_misses", m.toInt)
          Stats.addMetric("cache_accesses", a.toInt)
          log.info("simcache interval %s %s %s".format(m, a, r))
        }
      }
    }
    node
  }
}