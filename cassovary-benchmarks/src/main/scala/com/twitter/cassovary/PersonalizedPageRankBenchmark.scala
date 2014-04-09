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

import com.twitter.cassovary.graph.{GraphUtils, DirectedGraph}
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams

class PersonalizedPageRankBenchmark(graph : DirectedGraph,
                                    randomWalkParams : RandomWalkParams = RandomWalkParams(20, 0.3))
  extends OperationBenchmark {

  val graphUtils = new GraphUtils(graph)

  override def name = "Personalized PageRank"

  var nodesIterator = graph.iterator

  def startNode() : Int = {
    if (nodesIterator.hasNext) {
      nodesIterator.next().id
    } else {
      nodesIterator = graph.iterator
      nodesIterator.next().id
    }
  }

  def operation() {
    graphUtils.calculatePersonalizedReputation(startNode(), randomWalkParams)
  }
}
