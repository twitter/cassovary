package com.twitter.cassovary.util

import org.specs.Specification

class FastClockIntArrayCacheSpec extends Specification {
  "FastClockIntArrayCache" should {
    var off = new Array[(Long, Int)](5)
    off(1) = (0L, 2)
    off(2) = (0L, 2)
    off(4) = (0L, 1)

    doFirst {
      val writer = new EdgeShardsWriter("test-shards", 10)
      writer.writeIntegersAtOffset(1, 0, List(2, 3))
      writer.writeIntegersAtOffset(2, 0, List(3, 4))
      writer.writeIntegersAtOffset(4, 0, List(1))
      writer.close
    }

    "Work" in {
      val fc = new FastClockIntArrayCache("test-shards", 10, 4, 2, 3, off)
      fc.get(1)(0) mustEqual 2
      fc.get(1)(1) mustEqual 3
      fc.misses mustEqual 1
      fc.hits mustEqual 1
    }

    "Evict Properly" in {
      val fc = new FastClockIntArrayCache("test-shards", 10, 4, 2, 3, off)
      fc.get(1)
      fc.get(2)
      fc.get(1)
      fc.get(2)
      fc.misses mustEqual 4
      fc.hits mustEqual 0
      fc.get(4)
      fc.get(2)
      fc.get(4)
      fc.misses mustEqual 5
      fc.hits mustEqual 2
      fc.get(4)
      fc.get(4)
      fc.get(2)
      fc.misses mustEqual 5
      fc.hits mustEqual 5
    }

    "Is clock and not LRU" in {
      val fc = new FastClockIntArrayCache("test-shards", 10, 4, 2, 4, off)
      fc.get(1)
      fc.get(2)
      fc.get(1)

      fc.misses mustEqual 2
      fc.hits mustEqual 1

      val four = fc.get(4)
      val two = fc.get(2)

      fc.misses mustEqual 3 // Since clock evicts 1 instead of 2
      fc.hits mustEqual 2

      four(0) mustEqual 1
      two(0) mustEqual 3
      two(1) mustEqual 4
    }

  }
}
