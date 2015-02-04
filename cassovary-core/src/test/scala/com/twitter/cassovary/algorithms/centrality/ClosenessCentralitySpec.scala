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

import com.twitter.cassovary.graph.TestGraphs
import org.scalatest.{WordSpec, Matchers}

class ClosenessCentralitySpec extends WordSpec with Matchers {
  lazy val graph = TestGraphs.g6WithEmptyNodes

  "Closeness centrality" should {

    "return proper values when unnormalized" in {
      val cc = new ClosenessCentrality(graph, normalize = false)

      cc(graph.getNodeById(0).get)  shouldEqual 0.0
      cc(graph.getNodeById(1).get)  shouldEqual 0.0
      cc(graph.getNodeById(10).get) should be(0.625 +- .005)
      cc(graph.getNodeById(11).get) should be(0.455 +- .005)
      cc(graph.getNodeById(12).get) should be(0.385 +- .005)
      cc(graph.getNodeById(13).get) should be(0.500 +- .005)
      cc(graph.getNodeById(14).get) should be(0.455 +- .005)
      cc(graph.getNodeById(15).get) should be(0.625 +- .005)
    }

    "return proper values when normalized" in {
      val cc = new ClosenessCentrality(graph, normalize=true)
      val centrality = cc.recalculate()

      cc(graph.getNodeById(0).get)  shouldEqual 0.0
      cc(graph.getNodeById(1).get)  shouldEqual 0.0
      cc(graph.getNodeById(10).get) should be (0.446 +- .005)
      cc(graph.getNodeById(11).get) should be (0.325 +- .005)
      cc(graph.getNodeById(12).get) should be (0.275 +- .005)
      cc(graph.getNodeById(13).get) should be (0.357 +- .005)
      cc(graph.getNodeById(14).get) should be (0.324 +- .005)
      cc(graph.getNodeById(15).get) should be (0.446 +- .005)
    }
  }
}
