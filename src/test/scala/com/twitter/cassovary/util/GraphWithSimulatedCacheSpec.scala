package com.twitter.cassovary.util

import org.specs.Specification
import com.twitter.cassovary.graph.TestGraphs
import com.twitter.cassovary.graph.GraphDir
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.graph.GraphUtils

class GraphWithSimulatedCacheSpec extends Specification {
  val innerGraph = TestGraphs.g6
    
  "GraphWithSimulatedCache" should {
    var g:GraphWithSimulatedCache = null
    doBefore {
      g = new GraphWithSimulatedCache(innerGraph, 5, "lru", 5)
    }
    
    "echo the correct stats" in {
      g.getNodeById(1)
      g.getNodeById(2)
      g.getNodeById(3)
      g.getNodeById(4)
      g.getNodeById(5)
      g.getNodeById(6)
      g.getStats mustEqual (6, 6, 1.0)
      g.diffStat mustEqual (6, 6, 1.0)
      g.getNodeById(2)
      g.diffStat mustEqual (0, 1, 0.0)
    }
    
    "echo some diff stats for each PersonalizedReputation" in {
      val walkParams = RandomWalkParams(100, 0.1, None, None, Some(2), false, GraphDir.OutDir, true)
      val graphUtils = new GraphUtils(g)
      graphUtils.calculatePersonalizedReputation(10, walkParams)
      g.getStats mustEqual (4, 100, 0.04)
      graphUtils.calculatePersonalizedReputation(10, walkParams)
      g.getStats mustEqual (4, 200, 0.02)
    }
  }
  
  "GraphWithSimulatedVarCache" should {
    var g:GraphWithSimulatedVarCache = null
    doBefore {
      g = new GraphWithSimulatedVarCache(innerGraph, 5, "lru", 5)
    }
    
    "echo the correct stats" in {
      g.getNodeById(10)
      g.getNodeById(11)
      g.getNodeById(12)
      g.getStats mustEqual (3, 3, 1.0)
    }
    
    "echo some diff stats for each PersonalizedReputation" in {
      val walkParams = RandomWalkParams(100, 0.1, None, None, Some(2), false, GraphDir.OutDir, true)
      val graphUtils = new GraphUtils(g)
      graphUtils.calculatePersonalizedReputation(10, walkParams)
      val (m, a, r) = g.getStats
      a mustEqual 100
    }
  }
}