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

/**
 * Created by bmckown on 1/28/15.
 */
class DegreeCentralitySpec extends WordSpec with Matchers {

  "Degree Centrality" should {

    lazy val graph = TestGraphs.g6WithEmptyNodes

    "return in-degree centrality values that are less than one for each node" in {
      val idc = new DegreeCentrality(graph, GraphDir.InDir, true)
      val centrality = idc.centralityValues
      centrality(0)  shouldEqual 0.0
      centrality(1)  shouldEqual 0.0
      centrality(11) shouldEqual 0.2857142857142857
      centrality(12) shouldEqual 0.42857142857142855
      centrality(13) shouldEqual 0.14285714285714285
      centrality(14) shouldEqual 0.42857142857142855
      centrality(15) shouldEqual 0.14285714285714285
    }

    "return out-degree centrality values that are integer valued" in {
      val odc = new DegreeCentrality(graph, GraphDir.OutDir, false)
      val centrality = odc.centralityValues
      centrality(0)  shouldEqual 0.0
      centrality(1)  shouldEqual 0.0
      centrality(11) shouldEqual 2.0
      centrality(12) shouldEqual 1.0
      centrality(13) shouldEqual 2.0
      centrality(14) shouldEqual 1.0
      centrality(15) shouldEqual 2.0
    }
  }
}
