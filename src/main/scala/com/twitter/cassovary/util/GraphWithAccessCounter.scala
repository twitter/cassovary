package com.twitter.cassovary.util

import com.twitter.cassovary.graph.{Graph,DirectedGraph}
import net.lag.logging.Logger
import java.io._

class GraphWithAccessCounter(val g: DirectedGraph, val statsInterval: Int,
                             val outputDirectory: String) extends DirectedGraph {

  var counter = new Array[Int](g.maxNodeId+1)
  var accesses = 0
  var writes = 0

  protected val log = Logger.get

  override def getNodeById(id: Int) = {
    counter(id) += 1
    accesses += 1
    if (accesses % statsInterval == 0) {
      log.info("simcache now writing access stats")
      writeStats("%s/%s.txt".format(outputDirectory, writes))
      writes += 1
      log.info("simcache done writing access stats %s".format(writes))
      resetStats
    }

    g.getNodeById(id)
  }

  override def existsNodeId(id: Int) = g.existsNodeId(id)
  def nodeCount = g.nodeCount
  val storedGraphDir = g.storedGraphDir
  def edgeCount = g.edgeCount
  def iterator = g.iterator

  def writeStats(fileName: String) = {
    printToFile(new File(fileName))(p => {
      counter.foreach({ i => p.println(i) })
    })
  }

  def getStats = { counter }

  private def resetStats = { counter = new Array[Int](g.maxNodeId+1) }

  def graph = g

  private def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

}
