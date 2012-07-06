package com.twitter.cassovary.util

import com.twitter.cassovary.graph.{Graph,DirectedGraph}
import java.io._

class GraphWithAccessCounter(val g: DirectedGraph) extends Graph {

  var counter = new Array[Int](g.maxNodeId+1)

  override def getNodeById(id: Int) = {
    counter(id) += 1
    g.getNodeById(id)
  }

  override def existsNodeId(id: Int) = {
    g.existsNodeId(id)
  }

  def writeStats(fileName: String) = {
    printToFile(new File(fileName))(p => {
      counter.foreach({ i => p.println(i) })
    })
  }

  def getStats = {
    counter
  }

  def resetStats = {
    counter = new Array[Int](g.maxNodeId+1)
  }

  def graph = {
    g
  }

  private def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
}
