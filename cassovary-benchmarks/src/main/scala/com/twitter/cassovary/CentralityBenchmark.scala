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
package com.twitter.cassovary

import com.twitter.cassovary.algorithms.centrality.{ClosenessCentrality, DegreeCentrality}
import com.twitter.cassovary.graph.{GraphDir, Node, DirectedGraph}

sealed abstract class DegreeCentralityBenchmark(graph: DirectedGraph[Node], dir: GraphDir.GraphDir)
  extends OperationBenchmark {
  def operation(): Unit = {
    new DegreeCentrality(graph, dir)
  }
}

class InDegreeCentralityBenchmark(graph: DirectedGraph[Node])
  extends DegreeCentralityBenchmark(graph, GraphDir.InDir)

class OutDegreeCentralityBenchmark(graph: DirectedGraph[Node])
  extends DegreeCentralityBenchmark(graph, GraphDir.OutDir)

class ClosenessCentralityBenchmark(graph: DirectedGraph[Node])
  extends OperationBenchmark {
  def operation(): Unit = {
    new ClosenessCentrality(graph)
  }
}
