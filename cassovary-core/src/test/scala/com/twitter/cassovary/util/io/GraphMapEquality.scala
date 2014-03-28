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
package com.twitter.cassovary.util.io

import com.twitter.cassovary.graph.DirectedGraph
import org.specs.Specification

trait GraphMapEquality extends Specification {

  /**
   * Compares the nodes in a graph and those defined by the nodeMap (id -> ids of neighbors),
   * and ensures that these are equivalent
   * @param g DirectedGraph
   * @param nodeMap Map of node ids to ids of its neighbors
   */
  def nodeMapEquals(g: DirectedGraph, nodeMap: Map[Int, List[Int]]) = {
    g.foreach {
      node =>
        nodeMap.contains(node.id) mustBe true
        val neighbors = node.outboundNodes().toArray.sorted
        val nodesInMap = nodeMap(node.id).toArray.sorted
        neighbors.length mustBe nodesInMap.length
        neighbors.iterator.zip(nodesInMap.iterator).foreach {
          case (a, b) => a mustBe b
        }
    }
  }
}
