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
import node.ArrayBasedDirectedNode
import com.google.common.annotations.VisibleForTesting
import com.twitter.ostrich.stats.Stats
import java.util.concurrent.atomic.AtomicLong
import com.google.common.util.concurrent.MoreExecutors
import java.io._
import collection.mutable
import net.lag.logging.Logger
import com.google.common.cache.{LoadingCache, CacheLoader, Weigher, CacheBuilder}
import com.twitter.cassovary.util._
import scala.Some

private case class MaxIdsEdges(localMaxId:Int, localNodeWithoutOutEdgesMaxId:Int, numEdges:Int, nodeCount:Int)

/**
 * Methods for constructing a disk-cached directed graph
 */
object CachedDirectedGraph {

  private lazy val log = Logger.get("CachedDirectedGraph")

  def apply(iteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ], executorService: ExecutorService,
            storedGraphDir: StoredGraphDir, cacheType: String, cacheMaxNodes:Int, cacheMaxEdges:Int,
            shardDirectory: String, numShards: Int, numRounds: Int,
            useCachedValues: Boolean, cacheDirectory: String):CachedDirectedGraph = {

    var maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount = 0
    var numEdges = 0L

    log.info("---Beginning Load---")
    log.info("Cache Info: cacheMaxNodes is %s, cacheMaxEdges is %s, cacheType is %s".format(cacheMaxNodes, cacheMaxEdges, cacheType))
    log.info("Disk Shard Info: directory is %s, numShards is %s, numRounds is %s".format(shardDirectory, numShards, numRounds))
    log.info("useCachedValues is %s, cacheDirectory is %s".format(useCachedValues, cacheDirectory))

    // Step 0
    // Initialize serializer
    val serializer = new CachedDirectedGraphSerializer(cacheDirectory, useCachedValues)

    // Step 1
    // Find maxId, nodeWithOutEdgesMaxId, numEdges
    // Needed so that we can initialize arrays with the appropriate sizes
    log.info("Reading maxId and Calculating numEdges...")
    serializer.writeOrRead("step1.txt", { writer =>
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
      writer.integers(Seq(maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount)).longs(Seq(numEdges)).close
    }, { reader =>
      maxId = reader.int
      nodeWithOutEdgesMaxId = reader.int
      nodeWithOutEdgesCount = reader.int
      numEdges = reader.long
      reader.close
    })

    // Step 2
    // Generate the lookup table for nodeIdSet
    // Generate and store shard offsets and # of edges for each node
    log.info("Generating offset tables...")
    var nodeIdSet = new mutable.BitSet(maxId+1)
    var idToIntOffsetAndNumEdges = new Array[(Long,Int)](maxId+1)
    var edgeOffsets = new Array[AtomicLong](numShards)
    serializer.writeOrRead("step2.txt", { writer =>
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
      log.info("Writing offset tables to file...")
      writer.bitSet(nodeIdSet).arrayOfLongInt(idToIntOffsetAndNumEdges, nodeWithOutEdgesCount).atomicLongArray(edgeOffsets).close
    }, { reader =>
      nodeIdSet = reader.bitSet()
      idToIntOffsetAndNumEdges = reader.arrayOfLongInt()
      edgeOffsets = reader.atomicLongArray()
      reader.close
    })

    // Step 3
    // Allocating shards
    log.info("Allocating shards...")
    serializer.writeOrRead("step3.txt", { writer =>
      Stats.time("graph_load_allocating_shards") {
        val esw = new EdgeShardsWriter(shardDirectory, numShards)
        (0 until numShards).foreach { i =>
          esw.shardWriters(i).allocate(edgeOffsets(i).get * 4)
        }
        esw.close
      }
      writer.integers(Seq(1)).close
    }, { reader => reader.close })


    // Step 4x
    // Generate shards on disk in rounds
    // TODO maxSize of a shard is maxInt for now, not maxLong
    log.info("Writing to shards in rounds...")
    serializer.writeOrRead("step4.txt", { writer =>
      val shardSizes = edgeOffsets.map { i => i.get().toInt }
      val msw = new MemEdgeShardsWriter(shardDirectory, numShards, shardSizes, numRounds)
      (0 until numRounds).foreach { roundNo =>
        log.info("Beginning round %s...".format(roundNo))
        msw.startRound(roundNo)
        val (modStart, modEnd) = msw.roundRange
        Stats.time("graph_load_writing_round_to_shards") {
          def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) {
            iteratorFunc() foreach { item =>
              val id = item.id
              val shardId = id % numShards
              if (modStart <= shardId && shardId < modEnd) {
                val (edgeOffset, numEdges) = idToIntOffsetAndNumEdges(id)
                msw.writeIntegersAtOffsetFromOffset(id, edgeOffset.toInt, item.edges, 0, numEdges)
              }
            }
          }
          ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
            iteratorSeq, readOutEdges)
        }
        msw.endRound
      }
      writer.integers(Seq(1)).close
    }, { reader => reader.close })

//    // Step 4
//    // Generate shards on disk
//    log.info("Writing to shards...")
//    Stats.time("graph_load_writing_to_shards") {
//      def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) = {
//        val esw = new EdgeShardsWriter(shardDirectory, numShards)
//        iteratorFunc() foreach { item =>
//          val id = item.id
//          val (edgeOffset, _) = idToIntOffsetAndNumEdges(id)
//          esw.writeIntegersAtOffset(id, edgeOffset * 4, item.edges)
//        }
//        esw.close
//      }
//      ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
//        iteratorSeq, readOutEdges)
//    }

    // Step 5
    // Count number of nodes
    log.info("Counting total number of nodes...")
    var numNodes = 0
    serializer.writeOrRead("step5.txt", { writer =>
    Stats.time("graph_load_count_total_num_of_nodes") {
      for ( id <- 0 to maxId )
        if (nodeIdSet(id))
          numNodes += 1
      writer.integers(Seq(numNodes)).close
    }
    }, { reader =>
      numNodes = reader.int
      reader.close
    })

    // Do the below only if we need both directions
    if (storedGraphDir == StoredGraphDir.BothInOut) {
      throw new UnsupportedOperationException("BothInOut not supported at the moment")
      // Step S1 - Calculate in-edge sizes
      // Step S2 - WritableCache
    }

    log.info("---Ended Load!---")

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
      storedGraphDir: StoredGraphDir, cacheType:String, cacheDirectory:String = null):CachedDirectedGraph =
    apply(Seq(iteratorFunc), MoreExecutors.sameThreadExecutor(),
      storedGraphDir, cacheType, 2, 4, "temp-shards", 8, 2, true, cacheDirectory)
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

  protected def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
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

  def writeStats(fileName: String) {
    printToFile(new File(fileName))(p => {
      p.println("%s\t%s\t%s".format(misses, hits + misses, misses.toDouble / (hits+misses)))
    })
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

  def writeStats(fileName: String) {
    val stats = cache.stats()
    printToFile(new File(fileName))(p => {
      p.println("%s\t%s\t%s\t%s".format(stats.missCount(), stats.requestCount(), stats.missRate(), stats.averageLoadPenalty()))
    })
  }
}
