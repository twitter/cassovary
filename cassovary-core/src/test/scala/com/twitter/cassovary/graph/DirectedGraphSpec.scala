/*
 * Copyright 2014 Twitter, Inc.
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
package com.twitter.cassovary.graph

import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DirectedGraphSpec extends WordSpec with Matchers {

  val twoNodeGraph = TestGraphs.g2_mutual
  val sixNodeGraph = TestGraphs.g6

  "DirectedGraph" when {
    "two node with each following the other" should {
      "nodeCount and edgeCount be correct" in {
        twoNodeGraph.nodeCount shouldEqual 2
        twoNodeGraph.edgeCount shouldEqual 2
      }

      "maxNodeId be correct" in {
        twoNodeGraph.maxNodeId shouldEqual 2
      }

      "elements return all the nodes" in {
        twoNodeGraph.iterator.toList.map {_.id }.toSet shouldEqual Set(1, 2)
      }

      "getNodeById work in both positive and negative cases" in {
        twoNodeGraph.getNodeById(1) shouldEqual Some(TestNode(1, List(2), List(2)))
        twoNodeGraph.getNodeById(2) shouldEqual Some(TestNode(2, List(1), List(1)))
        twoNodeGraph.getNodeById(3) shouldEqual None
      }
    }

    "six node graph" should {
      "nodeCount and edgeCount be correct" in {
        sixNodeGraph.nodeCount shouldEqual 6
        sixNodeGraph.edgeCount shouldEqual 11
      }

      "maxNodeId be correct" in {
        sixNodeGraph.maxNodeId shouldEqual 15
      }

      "elements return all the nodes" in {
        sixNodeGraph.iterator.toList.map {_.id }.toSet shouldEqual Set(10, 11, 12, 13, 14, 15)
      }

      "existsNodeId work in both positive and negative cases" in {
        sixNodeGraph.existsNodeId(10) shouldEqual true
        sixNodeGraph.existsNodeId(11) shouldEqual true
        sixNodeGraph.existsNodeId(122) shouldEqual false
      }
    }
  }
}
