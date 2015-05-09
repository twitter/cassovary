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
package com.twitter.cassovary.graph.distributed

import com.twitter.cassovary.graph._

import scala.collection.immutable
import scala.util.Random

/**
 * Partitions a single NodeIdEdgesMaxId (assumed to contain edges in out direction)
 * to one or more instances.
 * @todo should allow node to be specified containing only inedges
 */
trait Partitioner {
  // instances are numbered 0..(numInstances-1)
  def numInstances: Int

  // a single `orig` node can be mapped to potentially multiple instances
  // @return a Map that maps instance number to a NodeIdEdgesMaxId in that instance
  def map(orig: NodeIdEdgesMaxId): immutable.Map[Int, NodeIdEdgesMaxId]
}

// Assigns nodes randomly to an instance
class RandomNodeMapper(val numInstances: Int, rand: Random = new Random()) extends Partitioner {
  def map(orig: NodeIdEdgesMaxId) = Map(rand.nextInt(numInstances) -> orig)
}

// Assigns nodes by a hash function of the source node's id
class HashSourceMapper(val numInstances: Int, hashNodeFn: Int => Int) extends Partitioner {
  def map(orig: NodeIdEdgesMaxId) = Map(hashNodeFn(orig.id) % numInstances -> orig)
}

// Assigns nodes by a hash function of the destination node's id. So,
// the edges out of node S can get split into multiple instances.
class HashDestMapper(val numInstances: Int, hashNodeFn: Int => Int) extends Partitioner {
  def map(orig: NodeIdEdgesMaxId) = {
    orig.edges.groupBy(x => hashNodeFn(x) % numInstances).mapValues { dstIds =>
      NodeIdEdgesMaxId(orig.id, dstIds)
    }
  }
}

class HashSourceAndDestMapper(val numInstances: Int, hashNodeFn: Int => Int) extends Partitioner {
  val byDest = new HashDestMapper(numInstances, hashNodeFn)
  val bySource = new HashSourceMapper(numInstances, hashNodeFn)

  // Assume orig has u -> {v0, v1} and instance numbered i gets v_i when mapped by dest.
  // If instance#0 gets 'u' by source, then it will get the full original node u -> {v0, v1, v2},
  // while instance#1 will get u -> {v1}. So we first map by dest and then overwrite for some nodes.
  def map(orig: NodeIdEdgesMaxId) = {
    byDest.map(orig) ++ bySource.map(orig)
  }
}

object Partitioner {
  /**
   * @return a map of instance number to the arraybaseddirectedgraph stored in that instance
   *         after partitioning `origGraph` based on `partitioner`
   */
  def fromArrayBasedDirectedGraph(origGraph: ArrayBasedDirectedGraph,
      partitioner: Partitioner): Map[Int, ArrayBasedDirectedGraph] = {
    val origNodesEdges = origGraph map { node =>
      val neighbors =
        if (origGraph.isDirStored(GraphDir.OutDir)) node.outboundNodes() else node.inboundNodes()
      NodeIdEdgesMaxId(node.id, neighbors.toArray)
    }

    val newNodesEdges: Iterable[(Int, NodeIdEdgesMaxId)] = origNodesEdges.flatMap(partitioner.map)
    val combinedMap: Map[Int, Iterable[NodeIdEdgesMaxId]] =
      newNodesEdges.groupBy(_._1).mapValues(x => x map (_._2))
    combinedMap mapValues { n => ArrayBasedDirectedGraph(Seq(n), 1, origGraph.storedGraphDir) }
  }
}
