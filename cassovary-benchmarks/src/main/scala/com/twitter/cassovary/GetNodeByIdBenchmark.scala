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

package com.twitter.cassovary

import com.twitter.cassovary.graph.DirectedGraph
import com.twitter.cassovary.graph.Node
import com.twitter.cassovary.graph.GraphDir.GraphDir
import scala.util.Random

/**
 * Simple benchmark that is intended to test speed of `getNodeById` and
 * getting neighbors of a `Node`.
 *
 * Performs approximately `maxSteps` `getNodeById` and `randomNeighbor` operations.
 */
class GetNodeByIdBenchmark(graph: DirectedGraph, maxSteps: Int,
                          direction: GraphDir)
  extends OperationBenchmark {

  val rng = new Random

  val nodes = graph.iterator.toArray

  def getRandomNode(): Node = {
    nodes(rng.nextInt(nodes.size))
  }

  var currentNode: Node = getRandomNode()

  override def operation(): Unit = {
    for (i <- 1 to maxSteps) {
      currentNode =
        currentNode.randomNeighbor(direction, rng) match {
          case Some(n) => graph.getNodeById(n).get
          case None => graph.getNodeById(getRandomNode().id).get
        }
    }
    currentNode = getRandomNode()
  }
}
