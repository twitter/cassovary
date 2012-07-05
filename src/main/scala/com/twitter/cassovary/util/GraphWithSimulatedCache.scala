package com.twitter.cassovary.util

import com.twitter.cassovary.graph.Graph

class GraphWithSimulatedCache(val g: Graph, val cacheSize: Int, val cacheMechanism: String) extends Graph {

  val cache = cacheMechanism match {
    case "clock" => new ClockSimulatedCache(cacheSize)
    case "mru" => new MRUSimulatedCache(cacheSize)
    case _ => new SimulatedCache(cacheSize)
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

  def graph = {
    g
  }

}
