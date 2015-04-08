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
package com.twitter.cassovary.algorithms.shortestpath

import com.twitter.cassovary.graph._
import org.scalatest.{Matchers, WordSpec}

class SingleSourceShortestPathSpec extends WordSpec with Matchers {
  lazy val graph = TestGraphs.g6

  "Single source shortest path" should {

    "return 0 paths when the source node is the same as the target node" in {
      val sp = new SingleSourceShortestPath(graph, 10)
      sp.shortestPaths(10).length shouldEqual 0
    }

    "return 3 paths when source node is 10 and target node is 14" in {
      val sp = new SingleSourceShortestPath(graph, 10)
      val paths = sp.shortestPaths(14)
      paths.length shouldEqual 3
      paths.head   shouldEqual Seq(10,11,14)
      paths(1)     shouldEqual Seq(10,12,14)
      paths(2)     shouldEqual Seq(10,13,14)
    }

    "return all paths from a given source node to all target nodes" in {
      val sp = new SingleSourceShortestPath(graph, 10)
      val allPaths = sp.allShortestPaths
      allPaths.size shouldEqual 6
      allPaths(10).length shouldEqual 0
      allPaths(14).length shouldEqual 3
      allPaths(15).length shouldEqual 3
      allPaths(14).head   shouldEqual Seq(10,11,14)
      allPaths(14)(1)     shouldEqual Seq(10,12,14)
      allPaths(14)(2)     shouldEqual Seq(10,13,14)
    }
  }
}
