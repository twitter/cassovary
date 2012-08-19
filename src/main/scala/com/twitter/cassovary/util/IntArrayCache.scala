package com.twitter.cassovary.util

/**
 * Definition of a cache of integer arrays
 */
trait IntArrayCache {
  /**
   * Get the int array associated with a particular id
   * @param id
   * @return
   */
  def get(id: Int):Array[Int]

  /**
   * Get statistics, namely
   * # of misses,
   * # of hits,
   * # of nodes in the cache,
   * # of edges in the cache
   * @return
   */
  def getStats:(Long, Long, Int, Long)

  /**
   * Spawn a child class that accesses the same cache in a thread-safe way
   * @return
   */
  def getThreadSafeChild: IntArrayCache
}

/**
 * Facilitate passing around of primitives
 */
class IntArrayCacheNumbers {
  var currNodeCapacity:Long = 0
  var currRealCapacity:Long = 0
  var hits, misses: Long = 0
}

class FakeIntArrayCache extends IntArrayCache {
  val emptyArray = Array.empty[Int]
  def get(id: Int) = emptyArray
  def getStats = throw new Exception("FakeIntArrayCache has no stats")
  def getThreadSafeChild = throw new Exception("FakeIntArrayCache can't get a child")
}