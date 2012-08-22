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

import util.Random
import concurrent.Lock
import com.twitter.cassovary.util.MultiDirIntShardsReader

object RandomizedIntArrayCache {
  def apply(shardDirectories: Array[String], numShards: Int,
            maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
            idToIntOffset: Array[Long], idToNumEdges: Array[Int]) = {

    new RandomizedIntArrayCache(shardDirectories, numShards,
      maxId, cacheMaxNodes, cacheMaxEdges,
      idToIntOffset, idToNumEdges,
      new MultiDirIntShardsReader(shardDirectories, numShards),
      new Array[Array[Int]](maxId + 1),
      new Array[Int](cacheMaxNodes),
      new IntArrayCacheNumbers,
      new Lock)
  }
}

class RandomizedIntArrayCache private(shardDirectories: Array[String], numShards: Int,
                                      maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                                      idToIntOffset: Array[Long], idToNumEdges: Array[Int],
                                      val reader: MultiDirIntShardsReader,
                                      val idToArray: Array[Array[Int]],
                                      val indexToId: Array[Int],
                                      val numbers: IntArrayCacheNumbers,
                                      val lock: Lock) extends IntArrayCache {

  val rand = new Random

  def getThreadSafeChild = new RandomizedIntArrayCache(shardDirectories, numShards,
    maxId, cacheMaxNodes, cacheMaxEdges,
    idToIntOffset, idToNumEdges,
    new MultiDirIntShardsReader(shardDirectories, numShards),
    idToArray, indexToId, numbers, lock)

  def get(id: Int) = {
    val a = idToArray(id)
    if (a != null) {
      // println(Thread.currentThread().getId+" Hit! " +id)
      numbers.hits += 1
      a
    }
    else {
      numbers.misses += 1
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
          numbers.currRealCapacity += numEdges

          // Evict as many elements as we need to get below capacity

          // Initial random eviction to add this id into the cache
          val randIndex = rand.nextInt(cacheMaxNodes)
          val idToEvict = indexToId(randIndex)
          indexToId(randIndex) = id
          // println(Thread.currentThread().getId+" Added " + id + " received " + idToEvict + " " + currRealCapacity + " " + cacheMaxEdges + " " + indexToId.deep.mkString("|"))
          if (idToEvict > 0 && idToEvict != id) {
            numbers.currRealCapacity -= idToArray(idToEvict).size
            idToArray(idToEvict) = null
            // println(Thread.currentThread().getId+" Evicted " + idToEvict + " " + currRealCapacity + " " + cacheMaxEdges + " " + indexToId.deep.mkString("|"))
          }

          // Subsequent evictions if we're above our limit
          while (numbers.currRealCapacity > cacheMaxEdges) {
            val randIndex = rand.nextInt(cacheMaxNodes)
            val idToEvict = indexToId(randIndex)
            if (idToEvict > 0) {
              indexToId(randIndex) = 0
              numbers.currRealCapacity -= idToArray(idToEvict).size
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
    println(numbers.currRealCapacity + " " + cacheMaxEdges + " " + indexToId)
  }

  def getStats = {
    (numbers.misses, numbers.hits, -1, numbers.currRealCapacity)
  }

}
