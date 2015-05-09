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
 * Calculate cosine similarity for a specific graph.
 * Cosine Similarity of two nodes is the size of their common neighbors
 * divided by the geometric mean of their degrees
 * @param graph The graph object to calculate the cosine similarity
 */
class CosineSimilarity(val graph: DirectedGraph[Node]) extends Similarity {

  def calculateSimilarity(dir: GraphDir, u: Int, v: Int): Double = {
    val neighborsOfU = getNeighbors(dir, u).get
    val neighborsOfV = getNeighbors(dir, v).get
    val commonNeighbors = neighborsOfU intersect neighborsOfV
    if(neighborsOfU.isEmpty || neighborsOfV.isEmpty) 0
    else commonNeighbors.size / ( math.sqrt(neighborsOfU.size) * math.sqrt(neighborsOfV.size) )
  }

}
