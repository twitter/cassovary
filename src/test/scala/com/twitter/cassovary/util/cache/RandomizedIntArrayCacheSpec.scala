package com.twitter.cassovary.util.cache

import org.specs.Specification
import com.twitter.cassovary.util.EdgeShardsWriter

class RandomizedIntArrayCacheSpec extends Specification {
  "RandomizedIntArrayCache" should {
    val offset = Array[Long](0L, 0L, 0L, 0L, 0L)
    val edges = Array[Int](0, 2, 2, 0, 1)

    doFirst {
      val writer = new EdgeShardsWriter("test-shards", 10)
      writer.writeIntegersAtOffset(1, 0, List(2, 3))
      writer.writeIntegersAtOffset(2, 0, List(3, 4))
      writer.writeIntegersAtOffset(4, 0, List(1))
      writer.close
    }

    "Generally work" in {
      val ri = RandomizedIntArrayCache(Array("test-shards"), 10, 4, 2, 3, offset, edges)
      ri.get(1)(0) mustEqual 2
      ri.get(1)(1) mustEqual 3
      val (rim, rih, _, _) = ri.getStats
      rim mustEqual 1
      rih mustEqual 1
    }

    "Successfully evict nodes" in {
      val ri = RandomizedIntArrayCache(Array("test-shards"), 10, 4, 2, 3, offset, edges)
      ri.get(1)
      ri.get(2)
      ri.get(1)
      ri.get(2)
      val (rim, rih, _, _) = ri.getStats
      rim must beGreaterThan(2L)
      rih must beLessThan(2L)
    }
  }
}
