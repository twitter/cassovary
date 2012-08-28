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
import com.twitter.cassovary.util.cache._
import node.DualCachedDirectedNode
import scala.Some

object FastDualCachedDirectedGraph {

  def apply(nodeIdSet:(Int => Boolean),
            cacheMaxNodes:Int, cacheMaxEdges:Long,
            shardDirectories:Array[String], inShardDirectories: Array[String], numShards:Int,
            idToIntOffsetOut:Array[Long], idToNumEdgesOut:Array[Int],
            idToIntOffsetIn:Array[Long], idToNumEdgesIn:Array[Int],
            maxId: Int, realMaxId: Int, realMaxIdOutEdges: Int, realMaxIdInEdges: Int,
            nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
            inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
            nodeCount: Int, edgeCount: Long, storedGraphDir: StoredGraphDir,
            cacheType: String = "lru", nodeType: String = "node"): CachedDirectedGraph = {

    def makeCache(shardDirs: Array[String], idToIntOffset: Array[Long], idToNumEdges: Array[Int], realMaxId: Int) = cacheType match {
      case "lru" => FastLRUIntArrayCache(shardDirs, numShards,
        realMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "bufflru" => BufferedFastLRUIntArrayCache(shardDirs, numShards,
        realMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "lockfreereadlru" => LocklessReadFastLRUIntArrayCache(shardDirs, numShards,
        realMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "random" => RandomizedIntArrayCache(shardDirs, numShards,
        realMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "locklessrandom" => LocklessRandomizedIntArrayCache(shardDirs, numShards,
        realMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "clock" => FastClockIntArrayCache(shardDirs, numShards,
        realMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case _ => throw new IllegalArgumentException("Unknown cacheType %s".format(nodeType))
    }

    val outCache = makeCache(shardDirectories, idToIntOffsetOut, idToNumEdgesOut, realMaxIdOutEdges)
    val inCache = makeCache(inShardDirectories, idToIntOffsetIn, idToNumEdgesIn, realMaxIdInEdges)

    println("Hi! DualCacheStats")
    println(idToIntOffsetIn.deep.mkString(", "))
    println(idToIntOffsetOut.deep.mkString(", "))
    println(idToNumEdgesIn.deep.mkString(", "))
    println(idToNumEdgesOut.deep.mkString(", "))
//    println(outCache.get(1).deep.mkString(", "))
//    println(inCache.get(1).deep.mkString(", "))

    nodeType match {
      case "node" => new FastDualCachedDirectedGraph(nodeIdSet,
        cacheMaxNodes, cacheMaxEdges,
        shardDirectories, numShards,
        idToIntOffsetOut, idToNumEdgesOut,
        idToIntOffsetIn, idToNumEdgesIn,
        maxId, realMaxId, realMaxIdOutEdges, realMaxIdInEdges,
        nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
        inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
        nodeCount, edgeCount, storedGraphDir, outCache, inCache)
      case _ => throw new IllegalArgumentException("Unknown nodeType %s asked of FastDualCachedDirectedGraph".format(nodeType))
    }
  }
}

class FastDualCachedDirectedGraph (val nodeIdSet:(Int => Boolean),
                                   val cacheMaxNodes:Int, val cacheMaxEdges:Long,
                                   val shardDirectories:Array[String], val numShards:Int,
                                   val idToIntOffsetOut:Array[Long], val idToNumEdgesOut:Array[Int],
                                   val idToIntOffsetIn:Array[Long], val idToNumEdgesIn:Array[Int],
                                   maxId: Int, realMaxId: Int, realMaxIdOutEdges: Int, realMaxIdInEdges: Int,
                                   nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
                                   inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
                                   val nodeCount: Int, val edgeCount: Long, val storedGraphDir: StoredGraphDir,
                                   val outCache: IntArrayCache, val inCache: IntArrayCache)
  extends CachedDirectedGraph(maxId, realMaxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
    inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount) {

  def getThreadSafeChild = new FastDualCachedDirectedGraph(nodeIdSet,
    cacheMaxNodes, cacheMaxEdges,
    shardDirectories, numShards,
    idToIntOffsetOut, idToNumEdgesOut,
    idToIntOffsetIn, idToNumEdgesIn,
    maxId, realMaxId, realMaxIdOutEdges, realMaxIdInEdges,
    nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
    inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
    nodeCount, edgeCount, storedGraphDir,
    outCache.getThreadSafeChild, inCache.getThreadSafeChild)

  def getMisses = outCache.getStats._1 + inCache.getStats._1

  def statsString: String = {
    val (misses, hits, currentSize, currRealCapacity) = outCache.getStats
    val (misses2, hits2, currentSize2, currRealCapacity2) = inCache.getStats
    "%s\t%s\t%s\t%s\t%s".format(misses, hits + misses, misses.toDouble / (hits+misses), currentSize, currRealCapacity) +
      "%s\t%s\t%s\t%s\t%s".format(misses2, hits2 + misses2, misses2.toDouble / (hits2+misses2), currentSize2, currRealCapacity2)
  }

  val shapeShiftingNode = DualCachedDirectedNode.shapeShifter(0, idToNumEdgesOut, outCache, idToNumEdgesIn, inCache)
  val someShapeShiftingNode = Some(shapeShiftingNode)

  def getNodeById(id: Int) = { // Stats.time("cached_get_node_by_id")
    if (id > realMaxId || !nodeIdSet(id)) { // Invalid id
      None
    }
    else {
      shapeShiftingNode.id = id
      someShapeShiftingNode
    }
  }

}