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
package com.twitter.cassovary.util.cache

import com.twitter.ostrich.stats.Stats
import concurrent.Lock
import com.twitter.cassovary.util.{LinkedIntIntMap, MultiDirEdgeShardsReader}

object FastLRUIntArrayCache {
  /**
   * Array-based LRU algorithm implementation
   * @param shardDirectories
   * @param numShards
   * @param maxId
   * @param cacheMaxNodes
   * @param cacheMaxEdges
   * @param idToIntOffset
   * @param idToNumEdges
   */
  def apply(shardDirectories: Array[String], numShards: Int,
            maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
            idToIntOffset: Array[Long], idToNumEdges: Array[Int]) = {

    new FastLRUIntArrayCache(shardDirectories, numShards,
      maxId, cacheMaxNodes, cacheMaxEdges,
      idToIntOffset, idToNumEdges,
      new MultiDirEdgeShardsReader(shardDirectories, numShards),
      new Array[Array[Int]](cacheMaxNodes + 1),
      new LinkedIntIntMap(maxId, cacheMaxNodes),
      new IntArrayCacheNumbers,
      new Lock
    )
  }
}

class FastLRUIntArrayCache private(shardDirectories: Array[String], numShards: Int,
                                   maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                                   idToIntOffset: Array[Long], idToNumEdges: Array[Int],
                                   val reader: MultiDirEdgeShardsReader,
                                   val indexToArray: Array[Array[Int]],
                                   val linkedMap: LinkedIntIntMap,
                                   val numbers: IntArrayCacheNumbers,
                                   val lock: Lock) extends IntArrayCache {

  def getThreadSafeChild = new FastLRUIntArrayCache(shardDirectories, numShards,
    maxId, cacheMaxNodes, cacheMaxEdges,
    idToIntOffset, idToNumEdges,
    new MultiDirEdgeShardsReader(shardDirectories, numShards),
    indexToArray, linkedMap, numbers, lock)

  def get(id: Int): Array[Int] = {
    lock.acquire
    if (linkedMap.contains(id)) {
      numbers.hits += 1
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
          numbers.misses += 1
          // Evict from cache
          numbers.currRealCapacity += numEdges
          while (linkedMap.getCurrentSize == cacheMaxNodes || numbers.currRealCapacity > cacheMaxEdges) {
            val oldIndex = linkedMap.getTailIndex
            numbers.currRealCapacity -= indexToArray(oldIndex).length
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
    (numbers.misses, numbers.hits, linkedMap.getCurrentSize, numbers.currRealCapacity)
  }
}
