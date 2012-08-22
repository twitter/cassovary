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

import org.specs.Specification
import com.twitter.cassovary.util.IntShardsWriter

class FastLRUIntArrayCacheSpec extends Specification {

  "FastLRUIntArrayCache" should {
    val offset = Array[Long](0L, 0L, 0L, 0L, 0L)
    val edges = Array[Int](0, 2, 2, 0, 1)

    doFirst {
      val writer = new IntShardsWriter("test-shards", 10)
      writer.writeIntegersAtOffset(1, 0, List(2, 3))
      writer.writeIntegersAtOffset(2, 0, List(3, 4))
      writer.writeIntegersAtOffset(4, 0, List(1))
      writer.close
    }

    "Work" in {
      val l = FastLRUIntArrayCache(Array("test-shards"), 10, 4, 2, 3, offset, edges)
      l.get(1)(0) mustEqual 2
      l.get(1)(1) mustEqual 3
      l.numbers.misses mustEqual 1
      l.numbers.hits mustEqual 1
    }

    "Evict in LRU order" in {
      val l = FastLRUIntArrayCache(Array("test-shards"), 10, 4, 2, 4, offset, edges)
      l.get(1)
      l.get(2)
      l.get(1)
      l.numbers.misses mustEqual 2
      l.numbers.hits mustEqual 1
      l.get(4)
      l.get(1)
      l.numbers.misses mustEqual 3
      l.numbers.hits mustEqual 2
    }

  }

}
