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

abstract class SimulatedCache(size: Int = 10) {
  var misses, accesses, prevMisses, prevAccesses: Long = 0
  val one:Short = 1

  def get(id: Int, eltSize: Short):Unit

  def get(id: Int, eltSize: Int):Unit = get(id, eltSize.toShort)
  def get(id: Int):Unit = get(id, one)
  
  // Get cumulative statistics
  def getStats = {
    (misses, accesses, misses.toDouble/accesses)
  }
  
  // Return the difference in misses, accesses between now and the last time this function was called
  def diffStat = {
    val (m, a) = (misses-prevMisses, accesses-prevAccesses)
    prevMisses = misses
    prevAccesses = accesses
    (m, a, m.toDouble/a)
  }
}

class FastLRUSimulatedCache(maxId: Int, size: Int = 10) extends SimulatedCache {
  // This version employs a doubly linked list as the cache
  // ids are mapped to locations in the cache, and the linked list keeps track
  // of the recentness of access.
  // The linked list is implemented as a set of arrays and pointers:
  // - cacheNext, cachePrev, cacheHead, cacheTail
  // Index 0 in all arrays should always be blank
  // - for convenience in referencing ids
  // - and because 0 indicates a null entry
  val cacheNext = new Array[Int](size+1) // cache next pointers
  val cachePrev = new Array[Int](size+1) // cache prev pointers
  var cacheHead, cacheTail = 1 // pointers to the head and tail of the cache
  var cacheSize = 0 // size of the cache
  val cacheToId = new Array[Int](size+1) // cache index -> id
  val cacheToSize = new Array[Short](size+1) // cache index -> element size
  val idToCache = new Array[Int](maxId+1) // id -> cache index

  def debug = {
    printf("Cache Head: %s, Tail: %s\n", cacheHead, cacheTail)
    println(cacheNext.toList)
    println(cachePrev.toList)
    println(cacheToId.toList)
    println(idToCache.toList)
  }

  def get(id: Int, eltSize:Short) = {
    // Increment accesses
    accesses += 1

    val idIndex = idToCache(id)

    // Check if id exists in cache
    if (idIndex == 0) { // If not, increment misses and check cache size
      misses += 1
      if (cacheSize + eltSize > size) {
        // Evict the tail by clearing it from cache, cacheToId, idToCache
        val prevTail = cacheTail
        val prevTailId = cacheToId(prevTail)
        val nextPrevTail = cacheNext(prevTail)
        
        // Adjust cache size
        cacheSize += eltSize - cacheToSize(prevTail)

        // Update idToCache
        idToCache(prevTailId) = 0 // cacheToId(prevTail) = 0 is not needed because prevTail = newHead
        // Update tail pointer
        cachePrev(nextPrevTail) = 0
        // Update cacheTail
        cacheTail = nextPrevTail
        // Add to head, add to cacheToId, add to idToCache
        val newHead = prevTail
        val prevHead = cacheHead
        // Update head pointers
        cacheNext(prevHead) = newHead
        cacheNext(newHead) = 0
        cachePrev(newHead) = prevHead
        // Update cacheToId and idToCache
        cacheToId(newHead) = id
        cacheToSize(newHead) = eltSize
        idToCache(id) = newHead
        // Update cacheHead
        cacheHead = newHead
      }
      else { // Update the head and modify cacheSize, add to cacheId, idToCache
        if (cacheSize == 0) { // The very first element
          cacheToId(1) = id
          cacheToSize(1) = eltSize
          idToCache(id) = 1
        }
        else { // Next elements until the cache is filled
          cacheHead += 1
          cachePrev(cacheHead) = cacheHead-1
          cacheNext(cacheHead-1) = cacheHead
          cacheToId(cacheHead) = id
          cacheToSize(cacheHead) = eltSize
          idToCache(id) = cacheHead
        }
        cacheSize += eltSize
      }
    }
    else {
      // If so, move it to the head of the linked list
      if (cacheHead != idIndex) {
        val prev = cachePrev(idIndex)
        val prevHead = cacheHead
        val next = cacheNext(idIndex)
        // Update prev and next's pointers
        cachePrev(next) = prev
        cacheNext(prev) = next
        // Update idIndex's own pointers
        cachePrev(idIndex) = prevHead
        cacheNext(idIndex) = 0
        // Update prevHead's pointers
        cacheNext(prevHead) = idIndex
        // Update cacheHead and cacheTail
        cacheHead = idIndex
        if (cacheTail == idIndex) cacheTail = next
      }
    }
  }

}

class MRUSimulatedCache(size: Int = 10) extends SimulatedCache {
  val cache = new mutable.HashMap[Int, Long]()
  
  def get(id: Int, eltSize:Short) = {
    throw new IllegalArgumentException("MRU doesn't work with variable element sizes")
  }
  
  override def get(id: Int) = {
    if (!cache.contains(id)) {
      misses += 1
      if (cache.size == size) {
        val (minKey, minValue) = cache.max(Ordering[Long].on[(_, Long)](_._2))
        cache.remove(minKey)
      }
    }
    cache.put(id, accesses)
    accesses += 1
  }
}

class ClockSimulatedCache(maxId: Int, size: Int = 10) extends SimulatedCache {
  // Note that idToCache uses 0 as a null marker, so that 1 must be subtracted from all values
  val idToCache = new Array[Int](maxId+1) // id -> (cache index + 1)
  val cacheToId = new Array[Int](size) // cache index -> id
  val cacheBit = new Array[Boolean](size) // cache index -> recently used bit
  var clockPointer = 0 // Clock hand
  var cacheSize = 0
  
  def get(id: Int, eltSize:Short) = {
    throw new IllegalArgumentException("Clock doesn't work with variable element sizes")
  }

  override def get(id: Int) = {
    accesses += 1

    // Examine recently used bit and set to false if true and advance
    // until false is seen, then return index
    def findFreeSlot = {
      while (cacheBit(clockPointer)) {
        cacheBit(clockPointer) = false
        clockPointer = (clockPointer + 1) % size
      }
      val returnVal = clockPointer
      clockPointer = (clockPointer + 1) % size
      returnVal
    }

    // If id doesn't exist in cache
    if (idToCache(id) == 0) {
      misses += 1
      val freeSlot = findFreeSlot
      if (cacheSize == size) idToCache(cacheToId(freeSlot)) = 0
      else cacheSize += 1
      idToCache(id) = freeSlot + 1
      cacheToId.update(freeSlot, id)
    }

    cacheBit(idToCache(id)-1) = true
  }
}
