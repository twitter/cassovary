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
package com.twitter.cassovary.algorithms.similarity

import com.twitter.cassovary.graph.{DirectedGraph, Node}
import com.twitter.cassovary.graph.GraphDir.GraphDir

/**
 * Calculate jaccard similarity for a specific graph.
 * Jaccard Similarity of two nodes is the size of their common neighbors
 * divided by the size of the union of their neighbors
 * @param graph The graph object to calculate the jaccard similarity
 */
class JaccardSimilarity(val graph: DirectedGraph[Node]) extends Similarity {

  def calculateSimilarity(u: Int, v: Int, dir: GraphDir): Double = {
    val neighborsOfU = getNeighbors(u, dir).get
    val neighborsOfV = getNeighbors(v, dir).get
    val commonNeighbors = neighborsOfU intersect neighborsOfV
    val unionNeighbors = neighborsOfU union neighborsOfV
    if (unionNeighbors.isEmpty) 0
    else commonNeighbors.size.toDouble / unionNeighbors.size
  }

}
