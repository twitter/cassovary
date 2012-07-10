package com.twitter.cassovary.util

import org.specs.Specification
import com.twitter.cassovary.graph.TestGraphs
import scala.io.Source

class GraphWithAccessCounterSpec extends Specification {
  "GraphWithAccessCounter" should {
    var g:GraphWithAccessCounter = null
    var n:Int = 0
    var m:Int = 0
    val innerGraph = TestGraphs.g6
    doBefore {
      g = new GraphWithAccessCounter(innerGraph, 5, "/Users/jcheng/temp/counter")
      n = innerGraph.randomNode
      m = innerGraph.randomNode
      while (n == m) n = innerGraph.randomNode
    }
    "count properly even with reset" in {
      g.getNodeById(n)
      g.getNodeById(n)
      g.getNodeById(n)
      g.getNodeById(m)
      (g.getStats)(n) mustEqual 3
      (g.getStats)(m) mustEqual 1
      g.getNodeById(n) // resets after this line
      g.getNodeById(n)
      (g.getStats)(n) mustEqual 1
      (g.getStats)(m) mustEqual 0
    }

    "write to file" in {
      g.getNodeById(n)
      g.getNodeById(m)
      g.getNodeById(n)
      g.getNodeById(n)
      g.getNodeById(m) // resets after this line
      val lines = Source.fromFile(g.outputDirectory+"/0.txt").getLines()
      lines.size mustEqual (innerGraph.maxNodeId+1)
    }
  }
}
