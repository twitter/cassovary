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

import com.twitter.cassovary.graph.StoredGraphDir._
import java.util.concurrent.{Future, ExecutorService}
import com.twitter.cassovary.util.{ExecutorUtils, EdgeShardsWriter, EdgeShardsReader, ArrayBasedLRUCache}
import node.ArrayBasedDirectedNode
import com.google.common.annotations.VisibleForTesting
import com.twitter.ostrich.stats.Stats
import java.util.concurrent.atomic.AtomicLong
import com.google.common.util.concurrent.MoreExecutors

private case class MaxIdsEdges(localMaxId:Int, localNodeWithoutOutEdgesMaxId:Int, numEdges:Int)

/**
 * Methods for constructing a disk-cached directed graph
 */
object CachedDirectedGraph {

  def apply(iteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ], executorService: ExecutorService,
            storedGraphDir: StoredGraphDir, shardDirectory: String, numShards: Int):CachedDirectedGraph = {

    var maxId, nodeWithOutEdgesMaxId = 0
    var numEdges = 0L

    // Step 1 - Find maxId, nodeWithOutEdgesMaxId, numEdges
    // Needed so that we can initialize arrays with the appropriate sizes
    val futures = {
      def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) = {
        var localMaxId = 0
        var localNodeWithOutEdgesMaxId = 0
        var numEdges = 0
        iteratorFunc().foreach { item =>
          localMaxId = localMaxId max item.maxId
          localNodeWithOutEdgesMaxId = localNodeWithOutEdgesMaxId max item.id
          numEdges += item.edges.length
        }
        MaxIdsEdges(localMaxId, localNodeWithOutEdgesMaxId, numEdges)
      }
      ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], MaxIdsEdges](executorService,
        iteratorSeq, readOutEdges)
    }
    futures.toArray map { future =>
      val f = future.asInstanceOf[Future[MaxIdsEdges]]
      val MaxIdsEdges(localMaxId, localNWOEMaxId, localNumEdges) = f.get
      maxId = maxId max localMaxId
      nodeWithOutEdgesMaxId = nodeWithOutEdgesMaxId max localNWOEMaxId
      numEdges += localNumEdges
    }

    // Step 2
    // Generate and store shard offsets and # of edges for each node
    // Generate shards themselves
    // Generate the lookup table for nodeIdSet
    val nodeIdSet = new Array[Byte](maxId+1)
    val idToIntOffsetAndNumEdges = new Array[(Long,Int)](maxId+1)
    val edgeOffsets = new Array[AtomicLong](numShards)

    (0 until numShards).foreach { i => edgeOffsets(i) = new AtomicLong() }
    Stats.time("Calculating offsets and writing to files") {
      def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) = {
        val esw = new EdgeShardsWriter(shardDirectory, numShards)
        iteratorFunc() foreach { item =>
          val id = item.id
          val shardNum = id % numShards
          val numEdges = item.edges.length
          val edgeOffset = edgeOffsets(shardNum).getAndAdd(numEdges)
          nodeIdSet(id) = 1
          item.edges foreach { edge => nodeIdSet(edge) = 1 }
          idToIntOffsetAndNumEdges(id) = (edgeOffset, numEdges)
          esw.writeIntegersAtOffset(id, edgeOffset * 4, item.edges)
        }
        esw.close
      }
      ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
        iteratorSeq, readOutEdges)
    }

    // Count number of nodes
    var numNodes = 0
    Stats.time("graph_load_count_total_num_of_nodes") {
      for ( id <- 0 to maxId )
        if (nodeIdSet(id) == 1)
          numNodes += 1
    }

    // Do the below only if we need both directions
    if (storedGraphDir == StoredGraphDir.BothInOut) {
      throw new UnsupportedOperationException("BothInOut not supported at the moment")

      // Step S1 - Calculate in-edge sizes

      // Step S2 - WritableCache

    }

    // Return our cool graph!
    new CachedDirectedGraph(nodeIdSet, 10, 100, shardDirectory, numShards,
      idToIntOffsetAndNumEdges, maxId, numNodes, numEdges, storedGraphDir)
  }

  @VisibleForTesting
  def apply(iteratorFunc: () => Iterator[NodeIdEdgesMaxId],
      storedGraphDir: StoredGraphDir):CachedDirectedGraph =
    apply(Seq(iteratorFunc), MoreExecutors.sameThreadExecutor(), storedGraphDir, "temp-shards", 10)
}

/**
 * This is an implementation of the directed graph trait backed by an array of nodes in memory,
 * or the cache, and shards on disk.
 * @param nodeIdSet nodes with either outgoing or incoming edges
 * @param cacheMaxNodes maximum number of nodes that the cache can store
 * @param cacheMaxEdges maximum number of edges that the cache can store
 * @param shardDirectory where shards live on disk
 * @param numShards number of shards to split into
 * @param idToIntOffsetAndNumEdges offset into a shard on disk and the number of edges
 * @param maxId max node id in the graph
 * @param nodeCount number of nodes in the graph
 * @param edgeCount number of edges in the graph
 * @param storedGraphDir the graph directions stored
 */
class CachedDirectedGraph private (
    val nodeIdSet:Array[Byte],
    val cacheMaxNodes:Int, val cacheMaxEdges:Int,
    val shardDirectory:String, val numShards:Int,
    val idToIntOffsetAndNumEdges:Array[(Long,Int)],
    maxId: Int, val nodeCount: Int, val edgeCount: Long,
    val storedGraphDir: StoredGraphDir) extends DirectedGraph {

  val cache = new ArrayBasedLRUCache[Node](cacheMaxNodes, cacheMaxEdges, maxId)
  val reader = new EdgeShardsReader(shardDirectory, numShards)
  val emptyArray = new Array[Int](0)

  override lazy val maxNodeId = maxId

  def iterator = (0 to maxId).flatMap(getNodeById(_)).iterator

  def getNodeById(id: Int) = {
    if (id > maxId || nodeIdSet(id) == 0) {
      None
    }
    else {
      if (cache.contains(id)) { // Cache hit
        cache.moveToHead(id)
        Some(cache.get(id))
      }
      else { // Cache miss
        idToIntOffsetAndNumEdges(id) match {
          case null => Some(ArrayBasedDirectedNode(id, emptyArray, storedGraphDir))
          case (offset, numEdges) => {
            val intArray = new Array[Int](numEdges)
            reader.readIntegersFromOffsetIntoArray(id, offset, numEdges, intArray, 0)
            val node = ArrayBasedDirectedNode(id, intArray, storedGraphDir)
            cache.addToHead(id, node)
            Some(node)
          }
        }
      }
    }
  }

}
