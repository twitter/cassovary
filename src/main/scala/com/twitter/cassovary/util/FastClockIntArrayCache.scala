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

import scala.collection.mutable

/**
 * Array-based Clock replacement algorithm implementation
 * @param shardDirectory
 * @param numShards
 * @param maxId
 * @param cacheMaxNodes
 * @param cacheMaxEdges
 * @param idToIntOffsetAndNumEdges
 */
class FastClockIntArrayCache(shardDirectory: String, numShards: Int,
                              maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                              idToIntOffsetAndNumEdges:Array[(Long,Int)]) extends IntArrayCache {

  val reader = new EdgeShardsReader(shardDirectory, numShards)
  val clockBits = new mutable.BitSet(cacheMaxNodes) // In-use bit
  val idBitSet = new mutable.BitSet(maxId+1) // Quick contains checking
  val indexToId = new Array[Int](cacheMaxNodes) // index -> id mapping
  val idToIndex = new Array[Int](maxId+1) // id -> index mapping
  val idToArray = new Array[Array[Int]](maxId+1) // id -> array mapping
  var pointer = 0 // clock hand
  var hits, misses: Long = 0
  var currNodeCapacity: Int = 0 // how many nodes are we storing?
  var currRealCapacity: Long = 0 // how many edges are we storing?

  /**
   * Replace the "oldest" entry with the given id
   * and array
   * @param id id to insert into the cache
   * @param array array to insert into the cache
   */
  private def replace(id:Int, array:Array[Int]) = {
    var replaced = false
    while(currNodeCapacity > cacheMaxNodes || currRealCapacity > cacheMaxEdges || !replaced) {
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

  def get(id: Int) = synchronized {
    if (idBitSet(id)) {
      hits += 1
      clockBits(idToIndex(id)) = true
      idToArray(id)
    }
    else {
      misses += 1
      idToIntOffsetAndNumEdges(id) match {
        case (offset, numEdges) => {
          // Read in array
          val intArray = new Array[Int](numEdges)
          reader.readIntegersFromOffsetIntoArray(id, offset * 4, numEdges, intArray, 0)
          // Evict from cache
          currNodeCapacity += 1
          currRealCapacity += numEdges
          replace(id, intArray)
          // Return array
          intArray
        }
        case null => throw new NullPointerException("FastLRUIntArrayCache idToIntOffsetAndNumEdges %s".format(id))
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
    (misses, hits, currNodeCapacity, currRealCapacity)
  }

}
