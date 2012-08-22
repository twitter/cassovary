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
package com.twitter.cassovary.graph

import org.specs.Specification

class TestGraphSpec extends Specification {

  "Erdos Renyi Graph" should {

    "Generate a complete graph with p = 1.0" in {
      val g = TestGraphs.generateRandomGraph(10, 1.0)
      g.nodeCount mustEqual 10
      g.foreach { node =>
        val nodeSet = (0 until 10).filter(i => i != node.id).toSet
        node.outboundNodes().toSet mustEqual nodeSet
        node.inboundNodes().toSet mustEqual nodeSet
      }
    }

    "Generate disconnected nodes with p = 0" in {
      val g = TestGraphs.generateRandomGraph(10, 0)
      g.edgeCount mustEqual 0
      g.foreach { node =>
        node.inboundCount mustEqual 0
        node.outboundCount mustEqual 0
      }
    }

  }

}
