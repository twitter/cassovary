/*
 * Copyright 2015 Twitter, Inc.
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
package com.twitter.cassovary.algorithms.centrality

import com.twitter.cassovary.graph._
import org.scalatest.{Matchers, WordSpec}

class DegreeCentralitySpec extends WordSpec with Matchers {

  lazy val graph = TestGraphs.g6WithEmptyNodes

  "Degree Centrality" should {

    "return in-degree centrality values that are less than one for each node" in {
      val idc = new DegreeCentrality(graph, GraphDir.InDir, true)
      idc(graph.getNodeById(0).get) shouldEqual 0.0
      idc(graph.getNodeById(1).get) shouldEqual 0.0
      idc(graph.getNodeById(11).get) should be (0.285 +- .001)
      idc(graph.getNodeById(12).get) should be (0.428 +- .001)
      idc(graph.getNodeById(13).get) should be (0.142 +- .001)
      idc(graph.getNodeById(14).get) should be (0.428 +- .001)
      idc(graph.getNodeById(15).get) should be (0.142 +- .001)
    }

    "return out-degree centrality values that are integer valued" in {
      val odc = new DegreeCentrality(graph, GraphDir.OutDir, false)
      odc(graph.getNodeById(0).get)  shouldEqual 0.0
      odc(graph.getNodeById(1).get)  shouldEqual 0.0
      odc(graph.getNodeById(11).get) shouldEqual 2.0
      odc(graph.getNodeById(12).get) shouldEqual 1.0
      odc(graph.getNodeById(13).get) shouldEqual 2.0
      odc(graph.getNodeById(14).get) shouldEqual 1.0
      odc(graph.getNodeById(15).get) shouldEqual 2.0
    }
  }
}
