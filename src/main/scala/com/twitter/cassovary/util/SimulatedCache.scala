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

class SimulatedCache(size: Int = 10) {
  val cache = new mutable.HashMap[Int, Long]()
  var misses: Long = 0
  var accesses: Long = 0

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

  def getStats(verbose: Boolean = true) = {
    (misses, accesses, misses.toDouble/accesses)
  }
}

class MRUSimulatedCache(size: Int = 10) extends SimulatedCache {
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
  val clockCache = new Array[Int](size)
  // Nodes in the cache
  val clockCount = new Array[Int](size)
  // Recently used bit
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
