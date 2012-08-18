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

import util.Random
import java.util.concurrent.atomic.{AtomicReference, AtomicLong, AtomicIntegerArray}

class RandomizedIntArrayCache(shardDirectories: Array[String], numShards: Int,
                              maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                              idToIntOffset:Array[Long], idToNumEdges:Array[Int]) extends IntArrayCache {

  val reader = new MultiDirEdgeShardsReader(shardDirectories, numShards)
  val rand = new Random
  val idToArray = Array.fill(maxId+1){ new AtomicReference[Array[Int]](null) }
  val indexToId = new AtomicIntegerArray(cacheMaxNodes)
  var hits, misses: AtomicLong = new AtomicLong
  var currRealCapacity: AtomicLong = new AtomicLong // how many edges are we storing?

  def get(id: Int) = {
    val a = idToArray(id).get
    if (a != null) {
      // println(Thread.currentThread().getId+" Hit! " +id)
      hits.incrementAndGet
      a
    }
    else {

      misses.incrementAndGet
      val numEdges = idToNumEdges(id)
      if (numEdges == 0) {
        throw new NullPointerException("FastLRUIntArrayCache idToIntOffsetAndNumEdges %s".format(id))
      }
      else {
        // println(Thread.currentThread().getId+" Missed! " +id)
        // Read in array
        val intArray = new Array[Int](numEdges)
        reader.readIntegersFromOffsetIntoArray(id, idToIntOffset(id) * 4, numEdges, intArray, 0)

        // Do this first so that once indexToId is set this element can be evicted
        val arr = idToArray(id).getAndSet(intArray)
        if (arr == null) {
          currRealCapacity.addAndGet(numEdges)
        }

        // Evict as many elements as we need to get below capacity

        // Initial random eviction to add this id into the cache
        val idToEvict = indexToId.getAndSet(rand.nextInt(cacheMaxNodes), id)
        // println(Thread.currentThread().getId+"Added " + id + " received " + idToEvict + " " + currRealCapacity.get + " " + cacheMaxEdges + " " + indexToId)
        if (idToEvict > 0) {
          val arr = idToArray(idToEvict).getAndSet(null)
          if (arr != null) {
            currRealCapacity.addAndGet(-arr.size)
            // println(Thread.currentThread().getId+"Evicted" + idToEvict + " " + currRealCapacity.get + " " + cacheMaxEdges + " " + indexToId)
          }
        }

        // Subsequent evictions if we're above our limit
        while (currRealCapacity.get > cacheMaxEdges) {
          var i = 1
          val idToEvict = indexToId.getAndSet(rand.nextInt(cacheMaxNodes), 0)
          if (idToEvict > 0) {
            val arr = idToArray(idToEvict).getAndSet(null)
            if (arr != null) {
              currRealCapacity.addAndGet(-arr.size)
              // println("Evicted" + idToEvict + " " + currRealCapacity.get + " " + cacheMaxEdges + " " + indexToId)
            }
          }
          i += 1
        }

        intArray
      }
    }
  }

  def debug = {
    println(currRealCapacity.get + " " + cacheMaxEdges + " " + indexToId)
  }

  def getStats = {
    (misses.get, hits.get, -1, currRealCapacity.get)
  }

}
