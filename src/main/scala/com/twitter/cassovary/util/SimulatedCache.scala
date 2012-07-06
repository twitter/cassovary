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
  var misses: Long = 0
  var accesses: Long = 0

  def get(id: Int)

  def getStats(verbose: Boolean = true) = {
    (misses, accesses, misses.toDouble/accesses)
  }
}

class LRUSimulatedCache(size: Int = 10) extends SimulatedCache {
  val cache = new mutable.HashMap[Int, Long]()

  def get(id: Int) = {
    if (!cache.contains(id)) {
      misses += 1
      if (cache.size == size) {
        val (minKey, _) = cache.min(Ordering[Long].on[(_, Long)](_._2))
        cache.remove(minKey)
      }
    }
    cache.put(id, accesses)
    accesses += 1
  }
}

class FastLRUSimulatedCache(maxId: Int, size: Int = 10) extends SimulatedCache {
  // cacheNext, cachePrev, cacheHead, cacheTail make up a non-circular doubly linked list
  // index 0 in all arrays should always be blank
  // for convenience in referencing ids and because 0 indicates a null entry
  val cacheNext = new Array[Int](size+1) // cache next pointers
  val cachePrev = new Array[Int](size+1) // cache prev pointers
  var cacheHead, cacheTail = 1 // pointers to the head and tail of the cache
  var cacheSize = 0 // size of the cache
  val cacheToId = new Array[Int](size+1) // cache index -> id
  val idToCache = new Array[Int](maxId+1) // id -> cache index

  def debug = {
    printf("Cache Head: %s, Tail: %s\n", cacheHead, cacheTail)
    println(cacheNext.toList)
    println(cachePrev.toList)
    println(cacheToId.toList)
    println(idToCache.toList)
  }

  override def get(id: Int) = {
    // Increment accesses
    accesses += 1

    val idIndex = idToCache(id)

    // Check if id exists in cache
    if (idIndex == 0) { // If not, increment misses and check cache size
      misses += 1
      if (cacheSize == size) {
        // Evict the tail by clearing it from cache, cacheToId, idToCache
        val prevTail = cacheTail
        val prevTailId = cacheToId(prevTail)
        val nextPrevTail = cacheNext(prevTail)

        // Update cacheToId and idToCache
        idToCache(prevTailId) = 0
        cacheToId(prevTail) = 0
        // Update tail pointers
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
        idToCache(id) = newHead
        // Update cacheHead
        cacheHead = newHead
      }
      else { // Update the head and increment cacheSize, add to cacheId, idToCache
        if (cacheSize == 0) { // The very first element
          cacheToId(1) = id
          idToCache(id) = 1
        }
        else { // Next elements until the cache is filled
          cacheHead += 1
          cachePrev(cacheHead) = cacheHead-1
          cacheNext(cacheHead-1) = cacheHead
          cacheToId(cacheHead) = id
          idToCache(id) = cacheHead
        }
        cacheSize += 1
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

class ClockSimulatedCache(size: Int = 10) extends SimulatedCache {
  val cache = new mutable.HashMap[Int, Int]()
  val clockCache = new Array[Int](size) // Nodes in the cache
  val clockCount = new Array[Int](size) // Recently used bit
  var clockPointer = 0 // Clock hand

  override def get(id: Int) = {
    // Examine recently used bit and set to 0 if 1 and advance
    // until 0 is seen, then return index
    def findFreeSlot = {
      while (clockCount(clockPointer) == 1) {
        clockCount.update(clockPointer, 0)
        clockPointer = (clockPointer + 1) % size
      }
      val returnVal = clockPointer
      clockPointer = (clockPointer + 1) % size
      returnVal
    }

    if (!cache.contains(id)) {
      val freeSlot = findFreeSlot
      misses += 1
      if (cache.size == size) {
        cache.remove(clockCache(freeSlot))
      }
      cache.put(id, freeSlot)
      clockCache.update(freeSlot, id)
    }

    clockCount.update(cache(id).toInt, 1)
    accesses += 1
  }
}
