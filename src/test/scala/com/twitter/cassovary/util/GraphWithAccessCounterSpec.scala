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

import com.twitter.cassovary.graph.TestGraphs
import org.specs.Specification
import scala.io.Source

class GraphWithAccessCounterSpec extends Specification {
  "GraphWithAccessCounter" should {
    var g:GraphWithAccessCounter = null
    var n:Int = 0
    var m:Int = 0
    val innerGraph = TestGraphs.g6

    doBefore {
      g = new GraphWithAccessCounter(innerGraph, 5, FileUtils.getTempDirectoryName)
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
      val lines = Source.fromFile(g.statsOutputDirectory+"/0.txt").getLines()
      lines.size mustEqual (innerGraph.maxNodeId+1)
    }
  }
}
