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
import concurrent.Lock

class RandomizedIntArrayCache(shardDirectories: Array[String], numShards: Int,
                              maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                              idToIntOffset:Array[Long], idToNumEdges:Array[Int]) extends IntArrayCache {

  val reader = new MultiDirEdgeShardsReader(shardDirectories, numShards)
  val rand = new Random
  val idToArray = new Array[Array[Int]](maxId+1)
  val indexToId = new Array[Int](cacheMaxNodes)
  var hits, misses: Long = 0L
  var currRealCapacity: Long = 0L // how many edges are we storing?
  var lock = new Lock

  def get(id: Int) = {
    val a = idToArray(id)
    if (a != null) {
      // println(Thread.currentThread().getId+" Hit! " +id)
      hits += 1
      a
    }
    else {
      misses += 1
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

        lock.acquire
        val a = idToArray(id)
        if (a != null) {
          lock.release
          a
        }
        else {
          idToArray(id) = intArray
          currRealCapacity += numEdges

          // Evict as many elements as we need to get below capacity

          // Initial random eviction to add this id into the cache
          val randIndex = rand.nextInt(cacheMaxNodes)
          val idToEvict = indexToId(randIndex)
          indexToId(randIndex) = id
          // println(Thread.currentThread().getId+" Added " + id + " received " + idToEvict + " " + currRealCapacity + " " + cacheMaxEdges + " " + indexToId.deep.mkString("|"))
          if (idToEvict > 0 && idToEvict != id) {
            currRealCapacity -= idToArray(idToEvict).size
            idToArray(idToEvict) = null
            // println(Thread.currentThread().getId+" Evicted " + idToEvict + " " + currRealCapacity + " " + cacheMaxEdges + " " + indexToId.deep.mkString("|"))
          }

          // Subsequent evictions if we're above our limit
          while (currRealCapacity > cacheMaxEdges) {
            val randIndex = rand.nextInt(cacheMaxNodes)
            val idToEvict = indexToId(randIndex)
            if (idToEvict > 0) {
              indexToId(randIndex) = 0
              currRealCapacity -= idToArray(idToEvict).size
              idToArray(idToEvict) = null
              // println(Thread.currentThread().getId+"Picked " + randIndex+ " Evicted " + idToEvict + " " + currRealCapacity + " " + cacheMaxEdges + " " + indexToId.deep.mkString("|"))
            }
            // println("Whiling!")
          }

          lock.release
          intArray
        }
      }
    }
  }

  def debug = {
    println(currRealCapacity + " " + cacheMaxEdges + " " + indexToId)
  }

  def getStats = {
    (misses, hits, -1, currRealCapacity)
  }

}
