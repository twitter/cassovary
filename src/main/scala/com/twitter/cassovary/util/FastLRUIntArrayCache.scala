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
 * @param idToIntOffsetAndNumEdges
 */
class FastLRUIntArrayCache(shardDirectory: String, numShards: Int,
                            maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                            idToIntOffsetAndNumEdges:Array[(Long,Int)]) extends IntArrayCache {

  val reader = new EdgeShardsReader(shardDirectory, numShards)
  val idToArray = new Array[Array[Int]](maxId+1)
  val linkedMap = new LinkedIntIntMap(maxId, cacheMaxNodes)
  var currRealCapacity: Long = 0
  var hits, misses: Long = 0

  def get(id: Int):Array[Int] = synchronized {
    if (linkedMap.contains(id)) {
      hits += 1
      linkedMap.moveToHead(id)
      idToArray(id)
    }
    else Stats.time("fastlru_miss") {
      misses += 1
      idToIntOffsetAndNumEdges(id) match {
        case (offset, numEdges) => {
          // Read in array
          val intArray = new Array[Int](numEdges)
          reader.readIntegersFromOffsetIntoArray(id, offset * 4, numEdges, intArray, 0)

          // Evict from cache
          currRealCapacity += numEdges
          while(linkedMap.getCurrentSize == cacheMaxNodes || currRealCapacity > cacheMaxEdges) {
            val oldId = linkedMap.removeFromTail()
            currRealCapacity -= idToArray(oldId).length
            idToArray(oldId) = null
          }

          linkedMap.addToHead(id)
          idToArray(id) = intArray
          intArray
        }
        case null => throw new NullPointerException("FastLRUIntArrayCache idToIntOffsetAndNumEdges %s".format(id))
      }
    }
  }

  def getStats = {
    (misses, hits, linkedMap.getCurrentSize, currRealCapacity)
  }

}
