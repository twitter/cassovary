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
 * An Int -> Int map with a backing doubly linked list
 * Especially useful in representing a cache
 * The linked list is implemented as a set of arrays and pointers
 * Any id added to the map should be > 0, as 0 indicates a null entry
 * @param maxId the maximum id of any element that will be inserted
 * @param size the size of this map
 */
class LinkedIntIntMap(maxId: Int, size: Int) {
  private val indexNext = new Array[Int](size+1) // cache next pointers
  private val indexPrev = new Array[Int](size+1) // cache prev pointers
  private var head, tail = 0 // pointers to the head and tail of the cache
  private var currentSize = 0
  private val indexToId = new Array[Int](size+1) // cache index -> id
  private val idToIndex = new Array[Int](maxId+1) // id -> cache index

  // Initialize a linked list of free indices
  private val freeIndices = new Array[Int](size+1)
  (0 until size+1).foreach { i => freeIndices(i) = i + 1}
  freeIndices(size) = 0

  /**
   * Add a free slot to the cache
   * @param index index of free slot
   */
  private def addToFree(index:Int):Unit = {
    currentSize -= 1
    freeIndices(index) = freeIndices(0)
    freeIndices(0) = index
  }

  /**
   * Get a free slot in the cache
   * @return index of free slot
   */
  private def popFromFree():Int = {
    currentSize += 1
    val popped = freeIndices(0)
    freeIndices(0) = freeIndices(popped)
    popped
  }

  /**
   * Remove the tail element of the list and return it
   * @return id of tail
   */
  def removeFromTail():Int = {
    val prevTail = tail
    val prevId = indexToId(prevTail)
    tail = indexNext(prevTail)
    addToFree(prevTail)
    idToIndex(prevId) = 0
    indexToId(prevTail) = 0
    indexPrev(tail) = 0
    prevId
  }

  def getTailIndex:Int = tail
  def getHeadIndex:Int = head
  def contains(id:Int):Boolean = idToIndex(id) != 0

  /**
   * Move an element to the front of the linked list
   * Cases - moving an element in between the head and tail, only 1 element,
   * moving the tail itself, moving the head itself
   * @param id element to move
   */
  def moveToHead(id:Int) {
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
   * Add an element to the head, removing elements if there are too many
   * @param id
   */
  def addToHead(id:Int) {
    // Cases - adding to 0 element, 1 element, >1 element list
    val prevHeadIdx = head
    while(currentSize == size) {
      removeFromTail()
    }

    head = popFromFree()
    idToIndex(id) = head
    indexNext(prevHeadIdx) = head
    indexPrev(head) = prevHeadIdx
    indexToId(head) = id
    indexNext(head) = 0

    if (currentSize == 1) tail = head // Since tail gets set to 0 when last elt removed
  }

}

/**
 * Basic methods for any simulated cache
 * @param size
 */
abstract class SimulatedCache(size: Int = 10) {
  var misses, accesses, prevMisses, prevAccesses: Long = 0
  val one:Short = 1

  def getAndUpdate(id: Int, eltSize: Int):Unit

  def getAndUpdate(id: Int):Unit = getAndUpdate(id, one)
  
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

/**
 * Cache implemented with essentially a linked hash map, itself implemented as
 * several int arrays. Cache evicts the least recently accessed id if the size of the
 * cache is reached. An optional size can be specified when adding to the cache,
 * so that it takes up more space

 * @param maxId The largest id that will be added to the cache
 * @param size Units of space available in the cache
 */
class FastLRUSimulatedCache(maxId: Int, size: Int = 10) extends SimulatedCache {
  var currRealCapacity = 0 // sum of sizes of elements in the cache
  val indexToSize = new Array[Int](size+1) // cache index -> element size
  val map = new LinkedIntIntMap(maxId, size)

  /**
   * The getAndUpdate function that you need that handles whether an id needs to be
   * added or simply can be retrieved from the cache
   * @param id the id of the element desired
   * @param eltSize the size of this element
   */
  def getAndUpdate(id: Int, eltSize:Int) = {
    accesses += 1
    if (!map.contains(id)) {
      misses += 1
      // Keep cache size down
      while(currRealCapacity + eltSize > size) {
        currRealCapacity -= indexToSize(map.getTailIndex)
        map.removeFromTail()
      }
      map.addToHead(id)
      currRealCapacity += eltSize
      indexToSize(map.getHeadIndex) = eltSize
    }
    else {
      map.moveToHead(id)
    }
  }

}

/**
 * Most recently used cache implementation
 * Significantly slower than FastLRU
 * @param size
 */
class MRUSimulatedCache(size: Int = 10) extends SimulatedCache {
  val cache = new mutable.HashMap[Int, Long]()
  
  def getAndUpdate(id: Int, eltSize:Int) = {
    throw new IllegalArgumentException("MRU doesn't work with variable element sizes")
  }
  
  override def getAndUpdate(id: Int) = {
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
  
  def getAndUpdate(id: Int, eltSize:Int) = {
    throw new IllegalArgumentException("Clock doesn't work with variable element sizes")
  }

  override def getAndUpdate(id: Int) = {
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
