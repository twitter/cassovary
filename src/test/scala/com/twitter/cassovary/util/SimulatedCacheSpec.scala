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

import org.specs.Specification

class SimulatedCacheSpec extends Specification {
  "Caching Mechanisms" should {
    def lruWorstCase(cache:SimulatedCache) = {
      cache.getAndUpdate(1)
      cache.getAndUpdate(2)
      cache.getAndUpdate(3)
      cache.getAndUpdate(4)
      cache.getAndUpdate(5)
      cache.getAndUpdate(6) // m, replaces 1
      cache.getAndUpdate(1) // m, replaces 2
      cache.getAndUpdate(2) // m, replaces 3
      cache.getAndUpdate(3) // m, replaces 4
      cache.getAndUpdate(4) // m, replaces 5
      cache.getAndUpdate(5) // m, replaces 6
      cache.getStats
    }

    def mruWorstCase(cache:SimulatedCache) = {
      cache.getAndUpdate(1)
      cache.getAndUpdate(2)
      cache.getAndUpdate(3)
      cache.getAndUpdate(4)
      cache.getAndUpdate(5)
      cache.getAndUpdate(6) // m, replaces 5
      cache.getAndUpdate(5) // m
      cache.getAndUpdate(6) // m
      cache.getAndUpdate(5) // m
      cache.getAndUpdate(6) // m
      cache.getStats
    }

    def clockCase(cache:SimulatedCache) = {
      cache.getAndUpdate(1) // m
      cache.getAndUpdate(2) // m
      cache.getAndUpdate(3) // m
      cache.getAndUpdate(4) // m
      cache.getAndUpdate(5) // m
      cache.getAndUpdate(6) // m, replaces 1
      cache.getAndUpdate(2) // h
      cache.getAndUpdate(7) // m, replaces 3
      cache.getAndUpdate(3) // m, replaces 4
      cache.getAndUpdate(2) // h
      cache.getStats
    }

    def clockDifferenceCase(cache:SimulatedCache) = {
      cache.getAndUpdate(1)
      cache.getAndUpdate(2)
      cache.getAndUpdate(3)
      cache.getAndUpdate(4)
      cache.getAndUpdate(5)
      cache.getAndUpdate(1)
      cache.getAndUpdate(6) // clock will replace 1, but lru will replace 2
      cache.getAndUpdate(1)
      cache.getStats
    }
    
    def sizableCase(cache:SimulatedCache) = {
      cache.getAndUpdate(1, 2) // m
      cache.getAndUpdate(2, 3) // m
      cache.getAndUpdate(1, 2) // h
      cache.getAndUpdate(2, 3) // h
      cache.getAndUpdate(3, 4) // m
      cache.getAndUpdate(1, 2) // m
      cache.getAndUpdate(2, 3) // m
      cache.getStats
    }
    
    def sizableCase2(cache:SimulatedCache) = {
      cache.getAndUpdate(1, 1) // m
      cache.getAndUpdate(2, 1) // m
      cache.getAndUpdate(3, 3) // m
      cache.getAndUpdate(1, 1) // h
      cache.getAndUpdate(2, 1) // h
      cache.getAndUpdate(4, 1) // m, clock replaces 1, lru replaces 3, mru replaces 2
      cache.getAndUpdate(5, 1) // m, clock replaces 2, lru replaces 1, mru replaces 4
      cache.getAndUpdate(2, 1) // lru hits, clock misses, replaces 3, mru misses, replaces 5
      cache.getAndUpdate(3, 3) // lru misses, replaces 4, clock misses, adds 3, mru hits
      cache.getStats
    }

    def sizableCase3(cache:SimulatedCache) = {
      cache.getAndUpdate(2, 1)
      cache.getAndUpdate(3, 1)
      cache.getAndUpdate(4, 1)
      cache.getAndUpdate(5, 1)
      cache.getAndUpdate(6, 1)
      cache.getAndUpdate(1, 5)
      cache.getAndUpdate(6, 1)
      cache.getAndUpdate(5, 1)
      cache.getAndUpdate(4, 1)
      cache.getAndUpdate(3, 1)
      cache.getAndUpdate(2, 1)
      cache.getStats
    }

    def sizableCase4(cache:SimulatedCache) = {
      cache.getAndUpdate(2, 1)
      cache.getAndUpdate(3, 1)
      cache.getAndUpdate(4, 1)
      cache.getAndUpdate(5, 1)
      cache.getAndUpdate(6, 1)
      cache.getAndUpdate(1, 1)
      cache.getAndUpdate(3, 1)
      cache.getAndUpdate(4, 1)
      cache.getAndUpdate(5, 1)
      cache.getAndUpdate(6, 1)
      cache.getStats
    }

    "Guava: replace the last recently seen node" in {
      var cache:GuavaSimulatedCache = null
      doBefore {
        cache = new GuavaSimulatedCache(20, { i => i }, 1000, "")
      }

      "set cacheFullPoint properly 1" in {
        cache.getAndUpdate(1)
        cache.getAndUpdate(20)
        cache.numAccessesAtFirstMiss mustEqual 2
      }

      "set cacheFullPoint properly 2" in {
        cache.getAndUpdate(1)
        cache.getAndUpdate(2)
        cache.getAndUpdate(30)
        cache.numAccessesAtFirstMiss mustEqual 3
      }

    }

    "Fast LRU: replace the last recently seen node" in {
      var cache:FastLRUSimulatedCache = null
      doBefore {
        cache = new FastLRUSimulatedCache(10, 5, 1000, "")
      }
      "work" in {
        cache.getAndUpdate(5)
        cache.currRealCapacity mustEqual 1
        cache.getAndUpdate(6)
        cache.currRealCapacity mustEqual 2
        cache.getAndUpdate(5)
        cache.currRealCapacity mustEqual 2
      }
      "replace 1 properly" in {
        cache.getAndUpdate(1)
        cache.getAndUpdate(2)
        cache.getAndUpdate(3)
        cache.getAndUpdate(4)
        cache.getAndUpdate(5)
        cache.getAndUpdate(6)
        cache.getAndUpdate(5)
        cache.currRealCapacity mustEqual 5
      }
      "do badly in lru worst case" in {
        lruWorstCase(cache) mustEqual (11, 11, 1.0)
        cache.numAccessesAtFirstMiss mustEqual 6
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
        cache.numAccessesAtFirstMiss mustEqual 5
      }
      "do badly in sizable case 2" in {
        sizableCase2(cache) mustEqual (6, 9, 6.0/9)
      }
      "do badly in sizable case 3" in {
        sizableCase3(cache) mustEqual (11, 11, 1.0)
      }
      "do badly in sizable case 4" in {
        sizableCase4(cache) mustEqual (6, 10, 0.6)
      }
      "call multiple times and then replace" in {
        cache.getAndUpdate(5, 2)
        cache.getAndUpdate(5, 2)
        cache.getAndUpdate(6, 3)
        cache.getAndUpdate(7, 1)
        cache.getStats mustEqual (3, 4, 0.75)
        cache.getAndUpdate(8, 1)
        cache.getAndUpdate(7, 1)
        cache.getAndUpdate(6, 3)
        cache.getAndUpdate(8, 1)
        cache.getStats mustEqual (4, 8, 0.5)
      }
    }

    "MRU: replace the most recently seen node" in {
      var cache:SimulatedCache = null
      doBefore {
        cache = new MRUSimulatedCache(5, 1000, "")
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
        cache = new ClockSimulatedCache(10, 5, 1000, "")
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
      var cache:SimulatedCache = new FastLRUSimulatedCache(10, 5, 1000, "")
      val (m, a, r) = cache.diffStat
      (m, a) mustEqual (0, 0)
      mruWorstCase(cache)
      cache.diffStat mustEqual (6, 10, 0.6)
      cache.getAndUpdate(1)
      cache.diffStat mustEqual (1, 1, 1.0)
    }
    
  }
}
