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
import com.google.common.cache._
import java.io.File
import com.twitter.ostrich.stats.Stats
import net.lag.logging.Logger

/**
 * A Simulated Cache, as the name implies, simulates a caching mechanism, and stores statistics
 * which can be later queried
 * Basic methods for any simulated cache
 * @param maxSize Maximum size of the cache (elements may have size >= 1)
 * @param statsInterval How often to write stats to disk (in intervals of # of accesses)
 * @param outputDirectory The directory to write stats to
 */
abstract class SimulatedCache(val maxSize: Int = 10, val statsInterval: Long, val outputDirectory: String) {
  protected var misses, accesses, prevMisses, prevAccesses, fullAccesses: Long = 0
  protected var writes = 0
  protected val log = Logger.get("SimulatedCache")

  /**
   * "Retrieve" an element of a given size
   * @param id Integer identifier of an element
   * @param eltSize Size of the element
   */
  def get(id: Int, eltSize: Int) {
    getAndUpdate(id, eltSize)
    checkAndWriteStats()
  }

  /**
   * "Retrieve" an element of size 1
   * @param id Integer identifier of an element
   */
  def get(id: Int) { get(id, 1) }

  /**
   * Sub-classes should override this
   */
  def getAndUpdate(id: Int, eltSize: Int)

  /**
   * Sub-classes should override this too if they do not support variable element size
   */
  def getAndUpdate(id: Int) { getAndUpdate(id, 1) }
  
  // Get cumulative statistics
  def getStats = {
    (misses, accesses, misses.toDouble/accesses)
  }

  def setNumAccessesAtFirstMissOnce() {
    if (fullAccesses == 0)
      fullAccesses = accesses
  }

  /**
   * When did the cache become full?
   * @return # of accesses when the cache became full
   */
  def numAccessesAtFirstMiss = {
    fullAccesses
  }

  def checkAndWriteStats() {
    if (accesses % statsInterval == 0) {
      val (m, a, r) = getStats
      Stats.addMetric("cache_misses", m.toInt)
      Stats.addMetric("cache_accesses", a.toInt)
      log.info("Misses/Access/Miss Ratio %s %s %s".format(m, a, r))
      writeStats("%s/%s.txt".format(outputDirectory, writes))
      writes += 1
    }
  }

  /**
   * Return the difference in misses, accesses between now and the last time this function was called
   */
  def diffStat = {
    val (m, a) = (misses-prevMisses, accesses-prevAccesses)
    prevMisses = misses
    prevAccesses = accesses
    (m, a, m.toDouble/a)
  }

  /**
   * Write out statistics to a file in the format
   * misses \t accesses \t missRatio \t cacheFullPoint
   * Note that cacheFullPoint is 0 if the cache isn't full yet
   * @param fileName Filename to write stats to
   */
  def writeStats(fileName: String) {
    val (misses, accesses, missRatio) = getStats
    FileUtils.printToFile(new File(fileName))(p => {
      p.println("%s\t%s\t%s\t%s".format(misses, accesses, missRatio, numAccessesAtFirstMiss))
    })
  }
}

/**
 * Cache implemented with essentially a linked hash map, itself implemented as
 * several int arrays. Cache evicts the least recently accessed id if the maxSize of the
 * cache is reached. An optional maxSize can be specified when adding to the cache,
 * so that it takes up more space

 * @param maxId The largest id that will be added to the cache
 * @param maxSize Units of space available in the cache
 */
class FastLRUSimulatedCache(maxId: Int, maxSize: Int = 10, statsInterval: Long, outputDirectory: String)
  extends SimulatedCache(maxSize, statsInterval, outputDirectory) {

  private var currRealCapacity = 0 // sum of sizes of elements in the cache
  private val indexToSize = new Array[Int](maxSize+1) // cache index -> element maxSize
  private val map = new LinkedIntIntMap(maxId, maxSize)

  /**
   * The getAndUpdate function that you need that handles whether an id needs to be
   * added or simply can be retrieved from the cache
   * @param id the id of the element desired
   * @param eltSize the maxSize of this element
   */
  def getAndUpdate(id: Int, eltSize: Int) {
    accesses += 1
    if (!map.contains(id)) {
      misses += 1
      // Keep cache maxSize down
      while(map.getCurrentSize == maxSize || currRealCapacity + eltSize > maxSize) {
        setNumAccessesAtFirstMissOnce()
        currRealCapacity -= indexToSize(map.getTailIndex)
        map.removeFromTail()
      }
      map.addToHeadAndNotExists(id)
      currRealCapacity += eltSize
      indexToSize(map.getHeadIndex) = eltSize
    }
    else {
      map.moveToHead(id)
    }
  }

  def getCurrCapacity = currRealCapacity

}

/**
 * Cache implemented using Google Guava's cache implementation
 *
 * @param maxSize Maximum size of the cache (elements may have size >= 1)
 * @param loader A (Int => Int) function that determines the size of an element.
 * @param statsInterval How often to write stats to disk (in intervals of # of accesses)
 * @param outputDirectory The directory to write stats to
 */
class GuavaSimulatedCache(maxSize: Int, loader: (Int => Int), statsInterval: Long, outputDirectory: String)
  extends SimulatedCache(maxSize, statsInterval, outputDirectory) {

  val removalListener = new RemovalListener[Int,Int] {
    def onRemoval(p1: RemovalNotification[Int, Int]) {
      setNumAccessesAtFirstMissOnce()
    }
  }

  val cache:LoadingCache[Int,Int] = CacheBuilder.newBuilder().maximumWeight(maxSize)
    .weigher(new Weigher[Int,Int] {
      def weigh(k:Int, v:Int):Int = v
    })
    .asInstanceOf[CacheBuilder[Int,Int]]
    .removalListener(removalListener)
    .build[Int,Int](new CacheLoader[Int,Int] {
      def load(k:Int):Int = {
        loader(k)
      }
    })

  override def getStats = {
    val stats = cache.stats()
    (stats.missCount(), stats.requestCount(), stats.missRate())
  }

  def getAndUpdate(id: Int, eltSize: Int) {
    accesses += 1
    cache.get(id)
  }
}

/**
 * Most recently used cache implementation
 * Significantly slower than FastLRU
 * @param maxSize Maximum size of the cache
 */
class MRUSimulatedCache(maxSize: Int = 10, statsInterval: Long, outputDirectory: String)
  extends SimulatedCache(maxSize, statsInterval, outputDirectory) {

  private val cache = new mutable.HashMap[Int, Long]()
  
  def getAndUpdate(id: Int, eltSize:Int) {
    throw new IllegalArgumentException("MRU doesn't work with variable element sizes")
  }

  override def numAccessesAtFirstMiss = {
    throw new IllegalArgumentException("Clock doesn't work with numAccessesAtFirstMiss")
  }
  
  override def getAndUpdate(id: Int) {
    if (!cache.contains(id)) {
      misses += 1
      if (cache.size == maxSize) {
        val (minKey, _) = cache.max(Ordering[Long].on[(_, Long)](_._2))
        cache.remove(minKey)
      }
    }
    cache.put(id, accesses)
    accesses += 1
  }
}

/**
 * Simulated cache using a Clock algorithm.
 * Whenever a cache hit occurs, set the cacheBit corresponding to that element.
 * Evict starting from where the clock hand is. When cacheBit is set, clear the cacheBit and proceed to the next element
 * If the cacheBit is not set, evict that element, and continue to the next element if we need more space.
 * In other words, cache elements have "two chances" before they are evicted.
 */
class ClockSimulatedCache(maxId: Int, maxSize: Int = 10, statsInterval: Long, outputDirectory: String)
  extends SimulatedCache(maxSize, statsInterval, outputDirectory) {

  // Note that idToCache uses 0 as a null marker, so that 1 must be subtracted from all values
  private val idToCache = new Array[Int](maxId+1) // id -> (cache index + 1)
  private val cacheToId = new Array[Int](maxSize) // cache index -> id
  private val cacheBit = new Array[Boolean](maxSize) // cache index -> recently used bit
  private var clockPointer = 0 // Clock hand
  private var cacheSize = 0
  
  def getAndUpdate(id: Int, eltSize:Int) {
    throw new IllegalArgumentException("Clock doesn't work with variable element sizes")
  }

  override def numAccessesAtFirstMiss = {
    throw new IllegalArgumentException("Clock doesn't work with numAccessesAtFirstMiss")
  }

  override def getAndUpdate(id: Int) {
    accesses += 1

    // Examine recently used bit and set to false if true and advance
    // until false is seen, then return index
    def findFreeSlot = {
      while (cacheBit(clockPointer)) {
        cacheBit(clockPointer) = false
        clockPointer = (clockPointer + 1) % maxSize
      }
      val returnVal = clockPointer
      clockPointer = (clockPointer + 1) % maxSize
      returnVal
    }

    // If id doesn't exist in cache
    if (idToCache(id) == 0) {
      misses += 1
      val freeSlot = findFreeSlot
      if (cacheSize == maxSize) idToCache(cacheToId(freeSlot)) = 0
      else cacheSize += 1
      idToCache(id) = freeSlot + 1
      cacheToId.update(freeSlot, id)
    }

    cacheBit(idToCache(id)-1) = true
  }
}
