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
package com.twitter.cassovary.algorithms

import com.twitter.cassovary.graph.{DirectedGraph, GraphDir}
import net.lag.logging.Logger
import com.twitter.cassovary.util.Progress

/**
 * Parameters for PageRank
 * @param dampingFactor Probability of NOT randomly jumping to another node
 * @param iterations How many PageRank iterations do you want?
 */
case class LabelPropagationParams(dampingFactor: Double = 0.85,
                          iterations: Option[Int] = Some(10))

/**
 * PageRank is a link analysis algorithm designed to measure the importance of nodes in a graph.
 * Popularized by Google.
 *
 * Unoptimized for now, and runs in a single thread.
 */
object LabelPropagation {

  /**
   * Execute label propagation with PageRank-like approach.
   * Note that the memory usage of this implementation is
   * proportional to the graph's maxId - you might want to renumber the
   * graph before running PageRank.
   * @param graph A DirectedGraph instance
   * @param params LabelPropagationParams from above
   * @return An array of doubles, with indices corresponding to node ids
   */
  def apply(graph: DirectedGraph, uidxTidx: Array[Int], params: LabelPropagationParams): Array[Array[Double]] = {
    val lp = new LabelPropagation(graph, uidxTidx, params)
    lp.run
  }

  /**
   * Execute a single iteration of label propagation, given the previous label propagation matrix
   * @param graph A DirectedGraph instance
   * @param params LabelPropagationParams
   * @param prArray A matrix of doubles, with rows corresponding to node ids and columns corresponding to topics.
   * @return The updated array
   */
  def iterate(graph: DirectedGraph, idxToTopic: Array[Int], params: LabelPropagationParams, prMatrix: Array[Array[Double]]) = {
    val lp = new LabelPropagation(graph, idxToTopic, params)
    lp.iterate(prMatrix: Array[Array[Double]])
  }
}

private class LabelPropagation(graph: DirectedGraph, uidxTidx: Array[Int], params: LabelPropagationParams) {

  private val log = Logger.get("LabelPropagation")

  val dampingFactor = params.dampingFactor
  val dampingAmount = (1.0D - dampingFactor) / graph.nodeCount

  /**
   * Execute Label Propagation with the desired params
   * @return A matrix of Label Propagation values
   */
  def run: Array[Array[Double]] = {

    // Let the user know if they can save memory!
    if (graph.maxNodeId.toDouble / graph.nodeCount > 1.1 && graph.maxNodeId - graph.nodeCount > 1000000)
      log.info("Warning - you may be able to reduce the memory usage of Label Propagation by renumbering this graph!")

    var beforePR = new Array[Array[Double]](graph.maxNodeId + 1);
    // Populate each row slice with 1 for column of user's topic, zeros otherwise.
    //for ((nodeArr, i) <- beforePR.view.zipWithIndex) beforePr(i) = new Array[Double](uidxTidx.size, 0.0); 
    //for ((nodeArr, i) <- beforePR.view.zipWithIndex) beforePr(i)(uidxTidx(i)) = 1.0;
    
    log.info("Initializing starting LabelPropagation...")
    val progress = Progress("pagerank_init", 65536, Some(graph.nodeCount))
    //val initialPageRankValue = 1.0D / graph.nodeCount
    graph.foreach { node =>
      //beforePR(node.id) = initialPageRankValue
      beforePR(node.id) = new Array[Double](uidxTidx.size); 
      if (uidxTidx(node.id) != -1)
        beforePR(node.id)(uidxTidx(node.id)) = 1.0;
      progress.inc
    }

    (0 until params.iterations.get).foreach { i =>
      log.info("Beginning %sth iteration".format(i))
      beforePR = iterate(beforePR)
    }

    beforePR
  }

  /**
   * Execute a single iteration of PageRank on the input array
   * @param beforePR PageRank values before the iteration
   * @return PageRank values after the iteration
   */
  def iterate(beforePR: Array[Array[Double]]) = {
    val afterPR = new Array[Array[Double]](graph.maxNodeId + 1)

    log.info("Calculating new Label Propagation values based on previous iteration...")
    val progress = Progress("pagerank_calc", 65536, Some(graph.nodeCount))
    graph.foreach { node =>
      //val givenPageRank = beforePR(node.id) / node.neighborCount(GraphDir.OutDir)
      //val givenTopicRank = beforePR(node.id) / node.neighborCount(GraphDir.OutDir)
      //beforePR(node.id).view.zipWithIndex.foreach { case (weight, topic_idx) =>
      //  beforePR(node.id)(topic_idx) /= node.neighborCount(GraphDir.OutDir)
      //}
      val givenTopicRank = beforePR(node.id) map (weight => weight / node.neighborCount(GraphDir.OutDir))
      node.neighborIds(GraphDir.OutDir).foreach { neighborId =>
        //afterPR(neighborId) += givenPageRank
        //afterPR(neighborId) += givenTopicRank
	afterPR(neighborId).view.zipWithIndex.foreach { case (weight, topic_idx) =>
	  afterPR(neighborId)(topic_idx) += givenTopicRank(topic_idx)
	}
      }
      progress.inc
    }

    log.info("Damping...")
    val progress_damp = Progress("pagerank_damp", 65536, Some(graph.nodeCount))
    if (dampingAmount > 0) {
      graph.foreach { node =>
        //afterPR(node.id) = dampingAmount + dampingFactor * afterPR(node.id)
	afterPR(node.id).view.zipWithIndex.foreach { case (weight, topic_idx) => 
	  afterPR(node.id)(topic_idx) = dampingAmount + dampingFactor * afterPR(node.id)(topic_idx)
	}
        progress_damp.inc
      }
    }
    afterPR
  }

}

