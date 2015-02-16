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
import scala.collection.mutable

class BetweennessCentralitySpec extends WordSpec with Matchers {
  lazy val graph = TestGraphs.g6


  "Betweenness centrality" should {
    "return correctly normalized values when requesting normalization" in {
      val bc = new BetweennessCentrality(graph)

      bc(graph.getNodeById(10).get) should be (.250 +- .005)
      bc(graph.getNodeById(11).get) should be (.133 +- .005)
      bc(graph.getNodeById(12).get) should be (.033 +- .005)
      bc(graph.getNodeById(13).get) should be (.033 +- .005)
      bc(graph.getNodeById(14).get) should be (.550 +- .005)
      bc(graph.getNodeById(15).get) should be (.550 +- .005)
    }

    "return somewhat large values when requesting no normalization" in {
      val bc = new BetweennessCentrality(graph, false)

      bc(graph.getNodeById(10).get) should be (5.000  +- .005)
      bc(graph.getNodeById(11).get) should be (2.666  +- .005)
      bc(graph.getNodeById(12).get) should be (0.666  +- .005)
      bc(graph.getNodeById(13).get) should be (0.666  +- .005)
      bc(graph.getNodeById(14).get) should be (11.000 +- .005)
      bc(graph.getNodeById(15).get) should be (11.000 +- .005)
    }
  }
}
