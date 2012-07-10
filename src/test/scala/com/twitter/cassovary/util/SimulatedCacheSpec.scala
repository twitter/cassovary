package com.twitter.cassovary.util

import org.specs.Specification

class SimulatedCacheSpec extends Specification {
  "Caching Mechanisms" should {
    def lruWorstCase(cache:SimulatedCache) = {
      cache.get(1)
      cache.get(2)
      cache.get(3)
      cache.get(4)
      cache.get(5)
      cache.get(6) // m, replaces 1
      cache.get(1) // m, replaces 2
      cache.get(2) // m, replaces 3
      cache.get(3) // m, replaces 4
      cache.get(4) // m, replaces 5
      cache.get(5) // m, replaces 6
      cache.getStats
    }

    def mruWorstCase(cache:SimulatedCache) = {
      cache.get(1)
      cache.get(2)
      cache.get(3)
      cache.get(4)
      cache.get(5)
      cache.get(6) // m, replaces 5
      cache.get(5) // m
      cache.get(6) // m
      cache.get(5) // m
      cache.get(6) // m
      cache.getStats
    }

    def clockCase(cache:SimulatedCache) = {
      cache.get(1) // m
      cache.get(2) // m
      cache.get(3) // m
      cache.get(4) // m
      cache.get(5) // m
      cache.get(6) // m, replaces 1
      cache.get(2) // h
      cache.get(7) // m, replaces 3
      cache.get(3) // m, replaces 4
      cache.get(2) // h
      cache.getStats
    }

    def clockDifferenceCase(cache:SimulatedCache) = {
      cache.get(1)
      cache.get(2)
      cache.get(3)
      cache.get(4)
      cache.get(5)
      cache.get(1)
      cache.get(6) // clock will replace 1, but lru will replace 2
      cache.get(1)
      cache.getStats
    }
    
    def sizableCase(cache:SimulatedCache) = {
      cache.get(1, 2) // m
      cache.get(2, 3) // m
      cache.get(1, 2) // h
      cache.get(2, 3) // h
      cache.get(3, 4) // m
      cache.get(1, 2) // m
      cache.get(2, 3) // m
      cache.getStats
    }
    
    def sizableCase2(cache:SimulatedCache) = {
      cache.get(1, 1) // m
      cache.get(2, 1) // m
      cache.get(3, 3) // m
      cache.get(1, 1) // h
      cache.get(2, 1) // h
      cache.get(4, 1) // m, clock replaces 1, lru replaces 3, mru replaces 2
      cache.get(5, 1) // m, clock replaces 2, lru replaces 1, mru replaces 4
      cache.get(2, 1) // lru hits, clock misses, replaces 3, mru misses, replaces 5
      cache.get(3, 3) // lru misses, replaces 4, clock misses, adds 3, mru hits
      cache.getStats
    }

    "Fast LRU: replace the last recently seen node" in {
      var cache:FastLRUSimulatedCache = null
      doBefore {
        cache = new FastLRUSimulatedCache(10, 5)
      }
      "work" in {
        cache.get(5)
        cache.cacheSize mustEqual 1
        cache.get(6)
        cache.cacheSize mustEqual 2
        cache.get(5)
        cache.cacheSize mustEqual 2
      }
      "replace 1 properly" in {
        cache.get(1)
        cache.get(2)
        cache.get(3)
        cache.get(4)
        cache.get(5)
        cache.get(6)
        cache.get(5)
        cache.cacheSize mustEqual 5
      }
      "do badly in lru worst case" in {
        lruWorstCase(cache) mustEqual (11, 11, 1.0)
      }
      "do well in mru worst case" in {
        mruWorstCase(cache) mustEqual (6, 10, 0.6)
      }
      "do okay in clock case" in {
        clockCase(cache) mustEqual (8, 10, 0.8)
      }
      "do okay in clockdiff case" in {
        clockDifferenceCase(cache) mustEqual (6, 8, 0.75)
      }
      "do okay in sizable case" in {
        sizableCase(cache) mustEqual (5, 7, 5.0/7)
      }
      "do badly in sizable case 2" in {
        sizableCase2(cache) mustEqual (6, 9, 6.0/9)
      }
    }

    "MRU: replace the most recently seen node" in {
      var cache:SimulatedCache = null
      doBefore {
        cache = new MRUSimulatedCache(5)
      }
      "do badly in mru worst case" in {
        mruWorstCase(cache) mustEqual (10, 10, 1.0)
      }
      "do well in lru worst case" in {
        lruWorstCase(cache) mustEqual (7, 11, 7.0/11)
      }
    }

    "Clock: replace nodes in a clock-like fashion" in {
      var cache:SimulatedCache = null
      doBefore {
        cache = new ClockSimulatedCache(10, 5)
      }
      "do okay in clock case" in {
        clockCase(cache) mustEqual (8, 10, 0.8)
      }
      "do badly in lru worst case" in {
        lruWorstCase(cache) mustEqual (11, 11, 1.0)
      }
      "do okay in mru worst case" in {
        mruWorstCase(cache) mustEqual (6, 10, 0.6)
      }
      "do differently in clockdiff case" in {
        clockDifferenceCase(cache) mustEqual (7, 8, 7.0/8)
      }
    }
    
    "diffstat should work" in {
      var cache:SimulatedCache = new FastLRUSimulatedCache(10, 5)
      val (m, a, r) = cache.diffStat
      (m, a) mustEqual (0, 0)
      mruWorstCase(cache)
      cache.diffStat mustEqual (6, 10, 0.6)
      cache.get(1)
      cache.diffStat mustEqual (1, 1, 1.0)
    }
    
  }
}
