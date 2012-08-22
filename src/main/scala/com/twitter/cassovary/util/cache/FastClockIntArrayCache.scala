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

import scala.collection.mutable
import com.twitter.cassovary.util.MultiDirIntShardsReader

/**
 * Array-based Clock replacement algorithm implementation
 * @param shardDirectories
 * @param numShards
 * @param maxId
 * @param cacheMaxNodes
 * @param cacheMaxEdges
 * @param idToIntOffset
 * @param idToNumEdges
 */
object FastClockIntArrayCache {
  def apply(shardDirectories: Array[String], numShards: Int,
            maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
            idToIntOffset: Array[Long], idToNumEdges: Array[Int]): FastClockIntArrayCache = {

    new FastClockIntArrayCache(shardDirectories, numShards,
      maxId, cacheMaxNodes, cacheMaxEdges,
      idToIntOffset, idToNumEdges,
      new MultiDirIntShardsReader(shardDirectories, numShards),
      new IntArrayCacheNumbers,
      new FastClockReplace(maxId, cacheMaxNodes, cacheMaxEdges))
  }
}

class FastClockReplace(maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long) {

  val clockBits = new mutable.BitSet(cacheMaxNodes)
  // In-use bit
  val idBitSet = new mutable.BitSet(maxId + 1)
  // Quick contains checking
  val indexToId = new Array[Int](cacheMaxNodes)
  // index -> id mapping
  val idToIndex = new Array[Int](maxId + 1)
  // id -> index mapping
  val idToArray = new Array[Array[Int]](maxId + 1)
  // id -> array mapping
  var pointer = 0
  // clock hand
  var currNodeCapacity: Int = 0
  // how many nodes are we storing?
  var currRealCapacity: Long = 0 // how many edges are we storing?

  /**
   * Replace the "oldest" entry with the given id
   * and array
   * @param id id to insert into the cache
   * @param array array to insert into the cache
   */
  def replace(id: Int, numEdges: Int, array: Array[Int]) = synchronized {
    currNodeCapacity += 1
    currRealCapacity += numEdges
    var replaced = false
    while (currNodeCapacity > cacheMaxNodes || currRealCapacity > cacheMaxEdges || !replaced) {
      // Find a slot which can be evicted
      while (clockBits(pointer) == true) {
        clockBits(pointer) = false
        pointer = (pointer + 1) % cacheMaxNodes
      }
      // Clear the old value if it exists
      val oldId = indexToId(pointer)
      if (idBitSet(oldId)) {
        currNodeCapacity -= 1
        currRealCapacity -= idToArray(oldId).length
        idBitSet(oldId) = false
        idToArray(oldId) = null
      }
      // Update cache with this but only at the first slot found
      if (!replaced) {
        idToArray(id) = array
        idBitSet(id) = true
        clockBits(pointer) = true
        indexToId(pointer) = id
        idToIndex(id) = pointer
        replaced = true
      }
      // Moving along...
      pointer = (pointer + 1) % cacheMaxNodes
    }
  }
}

class FastClockIntArrayCache private(shardDirectories: Array[String], numShards: Int,
                                     maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                                     idToIntOffset: Array[Long], idToNumEdges: Array[Int],
                                     val reader: MultiDirIntShardsReader,
                                     val numbers: IntArrayCacheNumbers,
                                     val replace: FastClockReplace) extends IntArrayCache {


  val idToArray = replace.idToArray
  val clockBits = replace.clockBits
  val idToIndex = replace.idToIndex
  val idBitSet = replace.idBitSet

  def getThreadSafeChild = new FastClockIntArrayCache(shardDirectories, numShards,
    maxId, cacheMaxNodes, cacheMaxEdges,
    idToIntOffset, idToNumEdges,
    new MultiDirIntShardsReader(shardDirectories, numShards),
    numbers, replace)

  def get(id: Int) = {
    val a = idToArray(id)
    if (a != null) {
      numbers.hits += 1
      clockBits(idToIndex(id)) = true
      a
    }
    else {
      numbers.misses += 1
      val numEdges = idToNumEdges(id)
      if (numEdges == 0) {
        throw new NullPointerException("FastLRUIntArrayCache idToIntOffsetAndNumEdges %s".format(id))
      }
      else {
        // Read in array
        val intArray = new Array[Int](numEdges)
        reader.readIntegersFromOffsetIntoArray(id, idToIntOffset(id), numEdges, intArray, 0)

        // Evict from cache
        replace.replace(id, numEdges, intArray)

        // Return array
        intArray
      }
    }
  }

  /**
   * Does the cache contain the given id?
   * @param id id to check
   * @return true or false
   */
  def contains(id: Int) = {
    idBitSet(id)
  }

  def getStats = {
    (numbers.misses, numbers.hits, replace.currNodeCapacity, replace.currRealCapacity)
  }

}
