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
  // number of instances
  def numInstances: Int

  // the function that maps a single node to potentially multiple instances
  def map(orig: NodeIdEdgesMaxId): immutable.Map[Int, NodeIdEdgesMaxId]
}

// Assigns nodes randomly to an instance
class RandomNodeMapper(val numInstances: Int, rand: Random = new Random()) extends Partitioner {
  def map(n: NodeIdEdgesMaxId) = Map(rand.nextInt(numInstances) -> n)
}

// Assigns nodes by a hash function of the source node's id
class HashSourceMapper(val numInstances: Int, hashNodeFn: Int => Int) extends Partitioner {
  def map(n: NodeIdEdgesMaxId) = Map(hashNodeFn(n.id) % numInstances -> n)
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

// Uses both by source and by dest partitioning
class HashSourceAndDestMapper(override val numInstances: Int, hashNodeFn: Int => Int)
    extends HashDestMapper(numInstances, hashNodeFn) {
  override def map(orig: NodeIdEdgesMaxId) = {
    // first do by dest, then overwrite the map value for that instance number
    // to which the source node would be mapped to
    super.map(orig) ++ Map(hashNodeFn(orig.id) % numInstances -> orig)
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