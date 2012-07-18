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

import org.specs.Specification
import com.twitter.cassovary.graph.TestGraphs
import com.twitter.cassovary.graph.GraphDir
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.graph.GraphUtils
import io.Source
import java.io.File
import com.twitter.io.Files

class GraphWithSimulatedCacheSpec extends Specification {
  val innerGraph = TestGraphs.g6

  "GraphWithSimulatedCache" should {
    doFirst {
      new File("temp-cache").mkdirs()
      new File("temp-cache2").mkdirs()
      new File("temp-cache4").mkdirs()
    }
    doLast {
      Files.delete(new File("temp-cache"))
      Files.delete(new File("temp-cache2"))
      Files.delete(new File("temp-cache4"))
    }

    "GraphWithSimulatedCache" in {
      var g:GraphWithSimulatedCache = null
      doBefore {
        g = new GraphWithSimulatedCache(innerGraph, 5, "lru", 5, "temp-cache")
      }

      "echo the correct stats" in {
        g.getNodeById(10)
        g.getNodeById(11)
        g.getNodeById(12)
        g.getNodeById(13)
        g.getNodeById(14)
        g.getNodeById(15)
        g.getNodeById(6) // not inside!
        g.getStats mustEqual (6, 6, 1.0)
        g.diffStat mustEqual (6, 6, 1.0)
        g.getNodeById(11)
        g.diffStat mustEqual (0, 1, 0.0)
        g.getStats mustEqual (6, 7, 6.0/7)
      }

      "echo some diff stats for each PersonalizedReputation and write out properly" in {
        val walkParams = RandomWalkParams(100, 0.1, None, None, Some(2), false, GraphDir.OutDir, true)
        val graphUtils = new GraphUtils(g)
        graphUtils.calculatePersonalizedReputation(10, walkParams)
        g.getStats mustEqual (4, 100, 0.04)
        graphUtils.calculatePersonalizedReputation(10, walkParams)
        g.getStats mustEqual (4, 200, 0.02)
        Source.fromFile(g.outputDirectory+"/39.txt").mkString mustEqual "4\t200\t0.02\n"
      }
    }

    "GraphWithSimulatedVarCacheGuava" in {
      var g:GraphWithSimulatedVarCache = null
      doBefore {
        g = new GraphWithSimulatedVarCache(innerGraph, 5, "guava", 5, "temp-cache4")
      }

      "do something" in {
        g.getNodeById(10)
        g.getNodeById(11)
        g.getNodeById(12)
        g.getStats mustEqual (3, 3, 1.0)
      }
    }

    "GraphWithSimulatedVarCache" in {
      var g:GraphWithSimulatedVarCache = null
      doBefore {
        g = new GraphWithSimulatedVarCache(innerGraph, 5, "lru", 5, "temp-cache2")
      }

      "echo the correct stats" in {
        g.getNodeById(10)
        g.getNodeById(11)
        g.getNodeById(12)
        g.getStats mustEqual (3, 3, 1.0)
      }

      "echo some diff stats for each PersonalizedReputation and also write out properly" in {
        val walkParams = RandomWalkParams(100, 0.1, None, None, Some(2), false, GraphDir.OutDir, true)
        val graphUtils = new GraphUtils(g)
        graphUtils.calculatePersonalizedReputation(10, walkParams)
        val (m, a, r) = g.getStats
        a mustEqual 100
        Source.fromFile(g.outputDirectory+"/19.txt").mkString mustEqual "%s\t%s\t%s\n".format(m, a, r)
      }
    }
  }
    

}