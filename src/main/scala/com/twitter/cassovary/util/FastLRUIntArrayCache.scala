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
package com.twitter.cassovary.util

import com.twitter.ostrich.stats.Stats
import concurrent.Lock

/**
 * Definition of a cache of integer arrays
 */
trait IntArrayCache {
  def get(id: Int):Array[Int]
  def getStats:(Long, Long, Int, Long)
}

/**
 * Array-based LRU algorithm implementation
 * @param shardDirectory
 * @param numShards
 * @param maxId
 * @param cacheMaxNodes
 * @param cacheMaxEdges
 * @param idToIntOffset
 * @param idToNumEdges
 */
class FastLRUIntArrayCache(shardDirectory: String, numShards: Int,
                            maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                            idToIntOffset:Array[Long], idToNumEdges:Array[Int]) extends IntArrayCache {

  val reader = new EdgeShardsReader(shardDirectory, numShards)
  val indexToArray = new Array[Array[Int]](cacheMaxNodes+1)
  val linkedMap = new LinkedIntIntMap(maxId, cacheMaxNodes)
  var currRealCapacity: Long = 0
  var hits, misses: Long = 0
  val lock = new Lock

  def get(id: Int):Array[Int] = {
    lock.acquire
    if (linkedMap.contains(id)) {
      hits += 1
      val idx = linkedMap.getIndexFromId(id)
      linkedMap.moveIndexToHead(idx)
      val a = indexToArray(idx)
      lock.release
      a
    }
    else Stats.time("fastlru_miss") {
      lock.release
      val numEdges = idToNumEdges(id)
      if (numEdges == 0) {
        throw new NullPointerException("FastLRUIntArrayCache idToIntOffsetAndNumEdges %s".format(id))
      }
      else {
        // Read in array
        val intArray = new Array[Int](numEdges)

        // Each EdgeShardReader is synchronized (i.e. 1 reader per shard)
        reader.readIntegersFromOffsetIntoArray(id, idToIntOffset(id) * 4, numEdges, intArray, 0)

        lock.acquire
        if (linkedMap.contains(id)) {
          val idx = linkedMap.getIndexFromId(id)
          linkedMap.moveIndexToHead(idx)
          val a = indexToArray(idx)
          lock.release
          a
        }
        else {
          misses += 1
          // Evict from cache
          currRealCapacity += numEdges
          while(linkedMap.getCurrentSize == cacheMaxNodes || currRealCapacity > cacheMaxEdges) {
            val oldIndex = linkedMap.getTailIndex
            currRealCapacity -= indexToArray(oldIndex).length
            // indexToArray(oldIndex) = null // Don't need this because it will get overwritten
            linkedMap.removeFromTail()
          }

          linkedMap.addToHead(id)
          indexToArray(linkedMap.getHeadIndex) = intArray
          lock.release
          intArray
        }
      }
    }
  }

  def getStats = {
    (misses, hits, linkedMap.getCurrentSize, currRealCapacity)
  }

}
