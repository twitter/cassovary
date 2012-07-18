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
import com.twitter.cassovary.util._
import node.ArrayBasedDirectedNode
import com.google.common.annotations.VisibleForTesting
import com.twitter.ostrich.stats.Stats
import java.util.concurrent.atomic.AtomicLong
import com.google.common.util.concurrent.MoreExecutors
import scala.Some
import java.io.File
import collection.mutable
import net.lag.logging.Logger
import com.google.common.cache.{LoadingCache, CacheLoader, Weigher, CacheBuilder}

private case class MaxIdsEdges(localMaxId:Int, localNodeWithoutOutEdgesMaxId:Int, numEdges:Int, nodeCount:Int)

/**
 * Methods for constructing a disk-cached directed graph
 */
object CachedDirectedGraph {

  private lazy val log = Logger.get("CacheDirectedGraph")

  def apply(iteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ], executorService: ExecutorService,
            storedGraphDir: StoredGraphDir, cacheMaxNodes:Int, cacheMaxEdges:Int,
            shardDirectory: String, numShards: Int, cacheType: String = "guava"):CachedDirectedGraph = {

    var maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount = 0
    var numEdges = 0L

    log.info("Cache Info: cacheMaxNodes is %s, cacheMaxEdges is %s, cacheType is %s".format(cacheMaxNodes, cacheMaxEdges, cacheType))
    log.info("Disk Shard Info: directory is %s, numShards is %s".format(shardDirectory, numShards))

    // Step 1
    // Find maxId, nodeWithOutEdgesMaxId, numEdges
    // Needed so that we can initialize arrays with the appropriate sizes
    log.info("Reading maxId and Calculating numEdges...")
    val futures = Stats.time("graph_load_reading_maxid_and_calculating_numedges") {
      def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) = {
        var localMaxId, localNodeWithOutEdgesMaxId, numEdges, nodeCount = 0
        iteratorFunc().foreach { item =>
          // Keep track of Max IDs
          localMaxId = localMaxId max item.maxId
          localNodeWithOutEdgesMaxId = localNodeWithOutEdgesMaxId max item.id
          // Update nodeCount and total edges
          numEdges += item.edges.length
          nodeCount += 1
        }
        MaxIdsEdges(localMaxId, localNodeWithOutEdgesMaxId, numEdges, nodeCount)
      }
      ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], MaxIdsEdges](executorService,
        iteratorSeq, readOutEdges)
    }
    futures.toArray map { future =>
      val f = future.asInstanceOf[Future[MaxIdsEdges]]
      val MaxIdsEdges(localMaxId, localNWOEMaxId, localNumEdges, localNodeCount) = f.get
      maxId = maxId max localMaxId
      nodeWithOutEdgesMaxId = nodeWithOutEdgesMaxId max localNWOEMaxId
      numEdges += localNumEdges
      nodeWithOutEdgesCount += localNodeCount
    }

    // Step 2
    // Generate the lookup table for nodeIdSet
    // Generate and store shard offsets and # of edges for each node
    log.info("Generating offset tables...")
    val nodeIdSet = new mutable.BitSet(maxId+1)
    val idToIntOffsetAndNumEdges = new Array[(Long,Int)](maxId+1)
    val edgeOffsets = new Array[AtomicLong](numShards)
    (0 until numShards).foreach { i => edgeOffsets(i) = new AtomicLong() }
    Stats.time("graph_load_generating_offset_tables") {
      def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) = {
        val esw = new EdgeShardsWriter(shardDirectory, numShards)
        iteratorFunc() foreach { item =>
          val id = item.id
          val itemEdges = item.edges.length
          // Update nodeId set
          nodeIdSet(id) = true
          item.edges foreach { edge => nodeIdSet(edge) = true }
          // Store offsets
          val edgeOffset = edgeOffsets(id % numShards).getAndAdd(itemEdges)
          idToIntOffsetAndNumEdges(id) = (edgeOffset, itemEdges)
        }
        esw.close
      }
      ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
        iteratorSeq, readOutEdges)
    }

    // Step 3
    // Allocating shards
    log.info("Allocating shards...")
    Stats.time("graph_load_allocating_shards") {
      val esw = new EdgeShardsWriter(shardDirectory, numShards)
      (0 until numShards).foreach { i =>
        esw.shardWriters(i).allocate(edgeOffsets(i).get)
      }
      esw.close
    }

    // Step 4
    // Generate shards on disk
    log.info("Writing to shards...")
    Stats.time("graph_load_writing_to_shards") {
      def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) = {
        val esw = new EdgeShardsWriter(shardDirectory, numShards)
        iteratorFunc() foreach { item =>
          val id = item.id
          val (edgeOffset, _) = idToIntOffsetAndNumEdges(id)
          esw.writeIntegersAtOffset(id, edgeOffset * 4, item.edges)
        }
        esw.close
      }
      ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
        iteratorSeq, readOutEdges)
    }

    // Step 5
    // Count number of nodes
    log.info("Counting total number of nodes...")
    var numNodes = 0
    Stats.time("graph_load_count_total_num_of_nodes") {
      for ( id <- 0 to maxId )
        if (nodeIdSet(id))
          numNodes += 1
    }

    // Do the below only if we need both directions
    if (storedGraphDir == StoredGraphDir.BothInOut) {
      throw new UnsupportedOperationException("BothInOut not supported at the moment")
      // Step S1 - Calculate in-edge sizes
      // Step S2 - WritableCache
    }

    // Return our cool graph!
    cacheType match {
      case "guava" => new GuavaCachedDirectedGraph(nodeIdSet, cacheMaxNodes+cacheMaxEdges,
        shardDirectory, numShards, idToIntOffsetAndNumEdges,
        maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
        numNodes, numEdges, storedGraphDir)
      case _ => new FastLRUCachedDirectedGraph(nodeIdSet, cacheMaxNodes, cacheMaxEdges,
        shardDirectory, numShards, idToIntOffsetAndNumEdges,
        maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
        numNodes, numEdges, storedGraphDir)
    }
  }

  @VisibleForTesting
  def apply(iteratorFunc: () => Iterator[NodeIdEdgesMaxId],
      storedGraphDir: StoredGraphDir, cacheType:String):CachedDirectedGraph =
    apply(Seq(iteratorFunc), MoreExecutors.sameThreadExecutor(),
      storedGraphDir, 2, 4, "temp-shards", 10, cacheType)
}

abstract class CachedDirectedGraph(maxId: Int,
    val nodeWithOutEdgesMaxId:Int, val nodeWithOutEdgesCount:Int) extends DirectedGraph {

  override lazy val maxNodeId = maxId

  def iterator = (0 to maxId).flatMap(getNodeById(_)).iterator

  val graphDir = storedGraphDir match {
    case(OnlyIn) => GraphDir.InDir
    case(OnlyOut) => GraphDir.OutDir
    case _ => throw new IllegalArgumentException("OnlyIn or OnlyOut accepted, but not both!")
  }

  def writeStats(fileName: String)
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
class FastLRUCachedDirectedGraph (
    val nodeIdSet:mutable.BitSet,
    val cacheMaxNodes:Int, val cacheMaxEdges:Int,
    val shardDirectory:String, val numShards:Int,
    val idToIntOffsetAndNumEdges:Array[(Long,Int)],
    maxId: Int, nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
    val nodeCount: Int, val edgeCount: Long, val storedGraphDir: StoredGraphDir)
    extends CachedDirectedGraph(maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount) {

  val reader = new EdgeShardsReader(shardDirectory, numShards)
  val emptyArray = new Array[Int](0)
  val indexToObject = new Array[Node](cacheMaxNodes+1)
  val cache = new LinkedIntIntMap(maxId, cacheMaxNodes)
  var currRealCapacity = 0
  var hits, misses = 0

  def getNodeById(id: Int) = Stats.time("cached_get_node_by_id") {
    if (id > maxId || !nodeIdSet(id)) { // Invalid id
      None
    }
    else {
      if (cache.contains(id)) { // Cache hit
        hits += 1
        cache.moveToHead(id)
        Some(indexToObject(cache.getIndexFromId(id)))
      }
      else { // Cache miss
        misses += 1
        idToIntOffsetAndNumEdges(id) match {
          case null => Some(ArrayBasedDirectedNode(id, emptyArray, storedGraphDir))
          case (offset, numEdges) => {
            // Read in the node from disk
            val intArray = new Array[Int](numEdges)
            Stats.time("read_integers_from_disk_shard") {
              reader.readIntegersFromOffsetIntoArray(id, offset, numEdges, intArray, 0)
            }
            val node = ArrayBasedDirectedNode(id, intArray, storedGraphDir)

            // Evict any items in the cache if needed and add
            val eltSize = node.neighborCount(graphDir)
            while(cache.getCurrentSize == cacheMaxNodes || currRealCapacity + eltSize > cacheMaxEdges) {
              currRealCapacity -= indexToObject(cache.getTailIndex).neighborCount(graphDir)
              cache.removeFromTail()
            }
            currRealCapacity += eltSize
            cache.addToHead(id)
            indexToObject(cache.getHeadIndex) = node

            Some(node)
          }
        }
      }
    }
  }

  def writeStats(fileName: String) = {
    printToFile(new File(fileName))(p => {
      p.println("%s\t%s\t%s".format(misses, hits + misses, misses.toDouble / (hits+misses)))
    })
  }

  private def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

}

/**
 * This is an implementation of the directed graph trait backed by a Google Guava cache
 * and shards on disk
 * @param nodeIdSet nodes with either outgoing or incoming edges
 * @param cacheMaxNodesAndEdges maximum number of nodes and edges that the cache can store
 * @param shardDirectory where shards live on disk
 * @param numShards number of shards to split into
 * @param idToIntOffsetAndNumEdges offset into a shard on disk and the number of edges
 * @param maxId max node id in the graph
 * @param nodeCount number of nodes in the graph
 * @param edgeCount number of edges in the graph
 * @param storedGraphDir the graph directions stored
 */
class GuavaCachedDirectedGraph (
    val nodeIdSet:mutable.BitSet,
    val cacheMaxNodesAndEdges:Int,
    val shardDirectory:String, val numShards:Int,
    val idToIntOffsetAndNumEdges:Array[(Long,Int)],
    maxId: Int, nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
    val nodeCount: Int, val edgeCount: Long, val storedGraphDir: StoredGraphDir)
    extends CachedDirectedGraph(maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount) {

  val reader = new EdgeShardsReader(shardDirectory, numShards)
  val emptyArray = new Array[Int](0)

  // Load Noder (haha)
  private def loadNode(id: Int):Node = {
    idToIntOffsetAndNumEdges(id) match {
      case null => ArrayBasedDirectedNode(id, emptyArray, storedGraphDir)
      case (offset, numEdges) => {
        // Read in the node from disk
        val intArray = new Array[Int](numEdges)
        reader.readIntegersFromOffsetIntoArray(id, offset, numEdges, intArray, 0)
        ArrayBasedDirectedNode(id, intArray, storedGraphDir)
      }
    }
  }

  // Guava Cache
  val cache:LoadingCache[Int,Node] = CacheBuilder.newBuilder()
    .maximumWeight(cacheMaxNodesAndEdges)
    .weigher(new Weigher[Int,Node] {
      def weigh(k:Int, v:Node):Int = v.neighborCount(graphDir) + 1
    })
    .asInstanceOf[CacheBuilder[Int,Node]]
    .build[Int,Node](new CacheLoader[Int,Node] {
      def load(k:Int):Node = loadNode(k)
    })

  def getNodeById(id: Int) = Stats.time("cached_get_node_by_id") {
    if (id > maxId || !nodeIdSet(id)) // Invalid id
      None
    else
      Some(cache.get(id))
  }

  def writeStats(fileName: String) = {
    val stats = cache.stats()
    printToFile(new File(fileName))(p => {
      p.println("%s\t%s\t%s\t%s".format(stats.missCount(), stats.requestCount(), stats.missRate(), stats.averageLoadPenalty()))
    })
  }

  private def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
}
