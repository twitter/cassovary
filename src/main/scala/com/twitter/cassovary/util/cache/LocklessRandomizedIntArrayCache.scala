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
import java.util.concurrent.atomic.{AtomicReferenceArray, AtomicLong, AtomicIntegerArray}
import com.twitter.cassovary.util.MultiDirEdgeShardsReader

object LocklessRandomizedIntArrayCache {
  def apply(shardDirectories: Array[String], numShards: Int,
            maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
            idToIntOffset: Array[Long], idToNumEdges: Array[Int]) = {
    val reader = new MultiDirEdgeShardsReader(shardDirectories, numShards)
    val rand = new Random
    val idToArray = new AtomicReferenceArray[Array[Int]](maxId + 1)
    val indexToId = new AtomicIntegerArray(cacheMaxNodes)
    var hits, misses: Long = 0L
    var currRealCapacity: AtomicLong = new AtomicLong // how many edges are we storing?

    new LocklessRandomizedIntArrayCache(shardDirectories, numShards,
      maxId, cacheMaxNodes, cacheMaxEdges,
      idToIntOffset, idToNumEdges,
      new MultiDirEdgeShardsReader(shardDirectories, numShards),
      new AtomicReferenceArray[Array[Int]](maxId + 1),
      new AtomicIntegerArray(cacheMaxNodes),
      new IntArrayCacheNumbers,
      new AtomicLong
    )
  }
}

class LocklessRandomizedIntArrayCache private(shardDirectories: Array[String], numShards: Int,
                                              maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                                              idToIntOffset: Array[Long], idToNumEdges: Array[Int],
                                              val reader: MultiDirEdgeShardsReader,
                                              val idToArray: AtomicReferenceArray[Array[Int]],
                                              val indexToId: AtomicIntegerArray,
                                              val numbers: IntArrayCacheNumbers,
                                              val currRealCapacity: AtomicLong) extends IntArrayCache {

  val mappy = Map(0 -> 0, 1 -> 3, 2 -> 1, 3 -> 1, 4 -> 0, 5 -> 1, 6 -> 4)

  val rand = new Random

  def getThreadSafeChild = new LocklessRandomizedIntArrayCache(shardDirectories, numShards,
    maxId, cacheMaxNodes, cacheMaxEdges,
    idToIntOffset, idToNumEdges,
    new MultiDirEdgeShardsReader(shardDirectories, numShards),
    idToArray, indexToId, numbers, currRealCapacity)

  def get(id: Int) = {
    val a = idToArray.get(id)
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

        val a = idToArray.getAndSet(id, intArray)
        if (a == null) {

          // val t = System.nanoTime()
          // println("Starting "+t)

          var change = numEdges

          // Location to place new id must be constant so that we don't evict an id multiple times
          val idToEvict = indexToId.getAndSet(id % cacheMaxNodes, id)
          // println(Thread.currentThread().getId+" Added " + id + " received " + idToEvict + " " + numbers.currRealCapacity + " " + cacheMaxEdges + " " + indexToId)
          if (idToEvict > 0) {
            // Deference idToEvict only if they aren't the same
            if (idToEvict != id) {
              val array = idToArray.getAndSet(idToEvict, null)
              if (array != null) {
                change -= array.size
              }
            }
            else {
              change -= numEdges
            }
            // if (mappy(indexToId.get(0)) + mappy(indexToId.get(1)) != currRealCapacity.get) {
            //  throw new Exception(id+" "+idToEvict+" "+indexToId + " " + currRealCapacity + "error!")
            //}
            // println(Thread.currentThread().getId+" Evicted " + idToEvict + " " + numbers.currRealCapacity + " " + cacheMaxEdges + " " + indexToId)
          }



          // Then keep evicting until we go under the capacity limit
          var whileCount = 0
          if (currRealCapacity.addAndGet(change) > cacheMaxEdges) {
            while (change > 0) {
              val idToEvict = indexToId.getAndSet(rand.nextInt(cacheMaxNodes), 0)
              if (idToEvict > 0) {
                val array = idToArray.getAndSet(idToEvict, null)
                if (array != null) {
                  currRealCapacity.addAndGet(-array.size)
                  change -= array.size
                }
                // println(Thread.currentThread().getId+" WEvicted " + idToEvict + " " + currRealCapacity + " " + cacheMaxEdges + " " + indexToId)
              }
              whileCount += 1
              if (whileCount % 1000 == 0) {
                println(Thread.currentThread().getId + " Stuck " + currRealCapacity + " " + cacheMaxEdges + " " + indexToId)
              }
            }
          }

          // println("Ending "+t)

        }

        intArray
      }
    }
  }

  def debug() {
    println(currRealCapacity + " " + cacheMaxEdges + " " + indexToId)
  }

  def getStats = {
    (numbers.misses, numbers.hits, -1, currRealCapacity.get)
  }

}
