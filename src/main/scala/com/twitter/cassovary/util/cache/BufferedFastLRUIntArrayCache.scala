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
import java.util.concurrent.locks.ReentrantReadWriteLock
import com.twitter.cassovary.util.{LinkedIntIntMap, MultiDirEdgeShardsReader}

object BufferedFastLRUIntArrayCache {
  def apply(shardDirectories: Array[String], numShards: Int,
            maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
            idToIntOffset: Array[Long], idToNumEdges: Array[Int]): BufferedFastLRUIntArrayCache = {

    new BufferedFastLRUIntArrayCache(shardDirectories, numShards,
      maxId, cacheMaxNodes, cacheMaxEdges,
      idToIntOffset, idToNumEdges,
      new MultiDirEdgeShardsReader(shardDirectories, numShards),
      new Array[Array[Int]](cacheMaxNodes + 1),
      new LinkedIntIntMap(maxId, cacheMaxNodes),
      new IntArrayCacheNumbers,
      new ReentrantReadWriteLock)
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
class BufferedFastLRUIntArrayCache private(shardDirectories: Array[String], numShards: Int,
                                           maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                                           idToIntOffset: Array[Long], idToNumEdges: Array[Int],
                                           val shardReader: MultiDirEdgeShardsReader,
                                           val indexToArray: Array[Array[Int]],
                                           val linkedMap: LinkedIntIntMap,
                                           val numbers: IntArrayCacheNumbers,
                                           val rw: ReentrantReadWriteLock) extends IntArrayCache {

  val bufferArray = new Array[Int](10)
  var bufferPointer = 0
  val reader = rw.readLock
  val writer = rw.writeLock

  def getThreadSafeChild = new BufferedFastLRUIntArrayCache(shardDirectories, numShards,
    maxId, cacheMaxNodes, cacheMaxEdges,
    idToIntOffset, idToNumEdges,
    new MultiDirEdgeShardsReader(shardDirectories, numShards),
    indexToArray, linkedMap, numbers, rw)

  def addToBuffer(threadId: Long, index: Int) {
    bufferArray(bufferPointer) = index
    bufferPointer += 1
  }

  def emptyBuffer(threadId: Long) = {
    for (i <- 0 until bufferPointer) {
      val id = bufferArray(i)
      if (linkedMap.contains(id)) {
        linkedMap.moveToHead(id)
      }
    }
    bufferPointer = 0
  }

  def get(id: Int): Array[Int] = {
    val threadId = Thread.currentThread.getId
    reader.lock()

    if (linkedMap.contains(id)) {
      numbers.hits += 1
      val idx = linkedMap.getIndexFromId(id)
      val a = indexToArray(idx)
      reader.unlock()

      // Add to buffer and empty if it's full
      addToBuffer(threadId, id)
      if (bufferPointer == 10) {
        writer.lock()
        emptyBuffer(threadId)
        writer.unlock()
      }

      a
    }
    else Stats.time("fastlru_miss") {
      reader.unlock()

      val numEdges = idToNumEdges(id)
      if (numEdges == 0) {
        throw new NullPointerException("FastLRUIntArrayCache idToIntOffsetAndNumEdges %s".format(id))
      }
      else {
        // Read in array
        val intArray = new Array[Int](numEdges)

        // Each EdgeShardReader is synchronized (i.e. 1 reader per shard)
        shardReader.readIntegersFromOffsetIntoArray(id, idToIntOffset(id) * 4, numEdges, intArray, 0)

        writer.lock()
        if (linkedMap.contains(id)) {
          writer.unlock()
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
            val oldIndex = linkedMap.getTailIndex
            numbers.currRealCapacity -= indexToArray(oldIndex).length
            // indexToArray(oldIndex) = null // Don't need this because it will get overwritten
            linkedMap.removeFromTail()
          }

          linkedMap.addToHead(id)
          indexToArray(linkedMap.getHeadIndex) = intArray
          writer.unlock()

          intArray
        }
      }
    }
  }

  def getStats = {
    (numbers.misses, numbers.hits, linkedMap.getCurrentSize, numbers.currRealCapacity)
  }

}
