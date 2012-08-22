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
import com.twitter.cassovary.util.{LinkedIntIntMap, MultiDirIntShardsReader}

object LocklessReadFastLRUIntArrayCache {
  def apply(shardDirectories: Array[String], numShards: Int,
            maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
            idToIntOffset: Array[Long], idToNumEdges: Array[Int]): LocklessReadFastLRUIntArrayCache = {

    new LocklessReadFastLRUIntArrayCache(shardDirectories, numShards,
      maxId, cacheMaxNodes, cacheMaxEdges,
      idToIntOffset, idToNumEdges,
      new MultiDirIntShardsReader(shardDirectories, numShards),
      new Array[Array[Int]](maxId + 1),
      new LinkedIntIntMap(maxId, cacheMaxNodes),
      new IntArrayCacheNumbers,
      new Lock)
  }
}

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
class LocklessReadFastLRUIntArrayCache private(shardDirectories: Array[String], numShards: Int,
                                               maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                                               idToIntOffset: Array[Long], idToNumEdges: Array[Int],
                                               val shardReader: MultiDirIntShardsReader,
                                               val idToArray: Array[Array[Int]],
                                               val linkedMap: LinkedIntIntMap,
                                               val numbers: IntArrayCacheNumbers,
                                               val lock: Lock) extends IntArrayCache {


  val bufferArray = new Array[Int](10)
  var bufferPointer = 0

  def getThreadSafeChild = new LocklessReadFastLRUIntArrayCache(shardDirectories, numShards,
    maxId, cacheMaxNodes, cacheMaxEdges,
    idToIntOffset, idToNumEdges,
    new MultiDirIntShardsReader(shardDirectories, numShards),
    idToArray, linkedMap, numbers, lock)

  def addToBuffer(threadId: Long, index: Int) {
    bufferArray(bufferPointer) = index
    bufferPointer += 1
  }

  def emptyBuffer(threadId: Long) = {
    var i = 0
    while (i < bufferPointer) {
      val id = bufferArray(i)
      if (linkedMap.contains(id)) {
        linkedMap.moveToHead(id)
      }
      i += 1
    }
    bufferPointer = 0
  }

  def get(id: Int): Array[Int] = {
    val threadId = Thread.currentThread.getId

    var a: Array[Int] = idToArray(id)
    if (a != null) {
      // Manage buffer
      numbers.hits += 1
      addToBuffer(threadId, id)
      if (bufferPointer == 10) {
        lock.acquire
        emptyBuffer(threadId)
        lock.release
      }

      a
    }
    else Stats.time("fastlru_miss") {

      val numEdges = idToNumEdges(id)
      if (numEdges == 0) {
        throw new NullPointerException("FastLRUIntArrayCache idToIntOffsetAndNumEdges %s".format(id))
      }
      else {
        // Read in array
        val intArray = new Array[Int](numEdges)

        // Each IntShardReader is synchronized (i.e. 1 reader per shard)
        shardReader.readIntegersFromOffsetIntoArray(id, idToIntOffset(id), numEdges, intArray, 0)

        lock.acquire
        if (linkedMap.contains(id)) {
          lock.release
          intArray
        }
        else {
          numbers.misses += 1

          //println("Going to empty...")
          // Empty buffer
          emptyBuffer(threadId)
          //println("Emptied buffer!")

          // Evict from cache
          numbers.currRealCapacity += numEdges
          while (linkedMap.getCurrentSize == cacheMaxNodes || numbers.currRealCapacity > cacheMaxEdges) {
            val oldId = linkedMap.getTailId
            numbers.currRealCapacity -= idToArray(oldId).length
            idToArray(oldId) = null // Don't need this because it will get overwritten
            linkedMap.removeFromTail()
          }

          linkedMap.addToHead(id)
          idToArray(id) = intArray

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
