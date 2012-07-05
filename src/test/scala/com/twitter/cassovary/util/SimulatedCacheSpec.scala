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
      cache.getStats()
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
      cache.getStats()
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
      cache.getStats()
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
      cache.getStats()
    }

    "LRU: replace the last recently seen node" in {
      var cache:SimulatedCache = null
      doBefore {
        cache = new SimulatedCache(5)
      }
      "do badly in lru worst case" in {
        lruWorstCase(cache) mustEqual (11, 11, 1.0) // misses, accesses, misses/accesses
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
        cache = new ClockSimulatedCache(5)
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
  }
}
