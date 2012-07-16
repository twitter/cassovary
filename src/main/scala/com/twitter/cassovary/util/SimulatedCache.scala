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

  def get(id: Int, eltSize: Int):Unit

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
  val indexNext = new Array[Int](size+1) // cache next pointers
  val indexPrev = new Array[Int](size+1) // cache prev pointers
  var head, tail = 0 // pointers to the head and tail of the cache
  var currRealCapacity = 0 // size of the cache
  var currIndexCapacity = 0
  val indexToId = new Array[Int](size+1) // cache index -> id
  val idToIndex = new Array[Int](maxId+1) // id -> cache index
  val indexToSize = new Array[Int](size+1) // cache index -> element size

  // Initialize a linked list of free indices
  val freeIndices = new Array[Int](size+1)
  (0 until size+1).foreach { i => freeIndices(i) = i + 1}
  freeIndices(size) = 0

  def debug = {
    printf("Cache Head: %s, Tail: %s\n", head, tail)
    println("next", indexNext.toList)
    println("prev", indexPrev.toList)
    println("cacheToId", indexToId.toList)
    println("idToCache", idToIndex.toList)
  }

  // Add an index to the free index list
  private def addToFree(index:Int):Unit = {
    currIndexCapacity -= 1
    freeIndices(index) = freeIndices(0)
    freeIndices(0) = index
  }

  // Pop an index from the free index list
  private def popFromFree():Int = {
    currIndexCapacity += 1
    val popped = freeIndices(0)
    freeIndices(0) = freeIndices(popped)
    popped
  }

  // Remove an item from the cache
  private def removeFromTail() {
    // Cases - removing from >1 and 1 element list
    val prevTail = tail
    val prevId = indexToId(prevTail)
    tail = indexNext(prevTail)
    currRealCapacity -= indexToSize(prevTail)
    addToFree(prevTail)
    idToIndex(prevId) = 0
    indexToId(prevTail) = 0
    indexPrev(tail) = 0
  }

  def moveToHead(id:Int) {
    // Cases to worry about:
    // - moving an element in between the head and tail
    // - only 1 element (head = tail)
    // - moving the tail itself
    // - moving the head itself
    val idx = idToIndex(id)
    if (idx != head) { // Implicitly means currIndexCapacity > 1
      val prevIdx = indexPrev(idx)
      val nextIdx = indexNext(idx)
      val prevHeadIdx = head

      // Point to the real tail if we moved the tail
      // can add in && currentIndexCapacity > 1 if there's no idx != head check
      if (tail == idx) tail = nextIdx

      // Update pointers
      indexNext(prevIdx) = nextIdx
      indexPrev(nextIdx) = prevIdx
      indexNext(idx) = 0
      indexPrev(idx) = prevHeadIdx
      indexNext(prevHeadIdx) = idx
      head = idx
    }
  }

  /**
   * Adds an element to the list, removing another
   * if there are too many elements
   */
  def addToHead(id:Int, eltSize:Int) {
    // Cases - adding to 0 element, 1 element, >1 element list
    val prevHeadIdx = head
    while(currIndexCapacity == size || currRealCapacity + eltSize > size) {
      removeFromTail()
    }

    currRealCapacity += eltSize
    head = popFromFree()
    idToIndex(id) = head
    indexToSize(head) = eltSize
    indexNext(prevHeadIdx) = head
    indexPrev(head) = prevHeadIdx
    indexToId(head) = id
    indexNext(head) = 0

    if (currIndexCapacity == 1) tail = head // Since tail gets set to 0 when last elt removed
  }

  def get(id: Int, eltSize:Int) = {
    println("Getting", id)
    accesses += 1

    if (idToIndex(id) == 0) {
      misses += 1
      addToHead(id, eltSize)
    }
    else {
      moveToHead(id)
    }
  }

}

class MRUSimulatedCache(size: Int = 10) extends SimulatedCache {
  val cache = new mutable.HashMap[Int, Long]()
  
  def get(id: Int, eltSize:Int) = {
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
  
  def get(id: Int, eltSize:Int) = {
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
