package com.twitter.cassovary.util.cache

import org.specs.Specification
import com.twitter.cassovary.util.EdgeShardsWriter

class FastClockIntArrayCacheSpec extends Specification {
  "FastClockIntArrayCache" should {
    val offset = Array[Long](0L, 0L, 0L, 0L, 0L)
    val edges = Array[Int](0, 2, 2, 0, 1)

    doFirst {
      val writer = new EdgeShardsWriter("test-shards", 10)
      writer.writeIntegersAtOffset(1, 0, List(2, 3))
      writer.writeIntegersAtOffset(2, 0, List(3, 4))
      writer.writeIntegersAtOffset(4, 0, List(1))
      writer.close
    }

    "Work" in {
      val fc = FastClockIntArrayCache(Array("test-shards"), 10, 4, 2, 3, offset, edges)
      fc.get(1)(0) mustEqual 2
      fc.get(1)(1) mustEqual 3
      fc.numbers.misses mustEqual 1
      fc.numbers.hits mustEqual 1
    }

    "Evict Properly" in {
      val fc = FastClockIntArrayCache(Array("test-shards"), 10, 4, 2, 3, offset, edges)
      fc.get(1)
      fc.get(2)
      fc.get(1)
      fc.get(2)
      fc.numbers.misses mustEqual 4
      fc.numbers.hits mustEqual 0
      fc.get(4)
      fc.get(2)
      fc.get(4)
      fc.numbers.misses mustEqual 5
      fc.numbers.hits mustEqual 2
      fc.get(4)
      fc.get(4)
      fc.get(2)
      fc.numbers.misses mustEqual 5
      fc.numbers.hits mustEqual 5
    }

    "Is clock and not LRU" in {
      val fc = FastClockIntArrayCache(Array("test-shards"), 10, 4, 2, 4, offset, edges)
      fc.get(1)
      fc.get(2)
      fc.get(1)

      fc.numbers.misses mustEqual 2
      fc.numbers.hits mustEqual 1

      val four = fc.get(4)
      val two = fc.get(2)

      fc.numbers.misses mustEqual 3 // Since clock evicts 1 instead of 2
      fc.numbers.hits mustEqual 2

      four(0) mustEqual 1
      two(0) mustEqual 3
      two(1) mustEqual 4
    }

  }
}
