package com.twitter.cassovary.util

import com.twitter.cassovary.graph.{DirectedGraph, Graph}
import java.io.File

class GraphWithSimulatedCache(val g: Graph, val cacheSize: Int, val cacheMechanism: String) extends Graph {

  val cache = cacheMechanism match {
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
    g.getNodeById(id)
  }

  override def existsNodeId(id: Int) = {
    g.existsNodeId(id)
  }

  def getStats(verbose: Boolean = true) = {
    cache.getStats(verbose)
  }

  def writeStats(fileName: String) = {
    val (misses, accesses, missRatio) = cache.getStats()
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
