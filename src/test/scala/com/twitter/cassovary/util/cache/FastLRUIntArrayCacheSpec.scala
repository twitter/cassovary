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
import com.twitter.cassovary.util.{FileUtils, IntShardsWriter}

class FastLRUIntArrayCacheSpec extends Specification {

  var l: IntArrayCache = _
  var l2: IntArrayCache = _
  val offset = Array[Long](0L, 0L, 0L, 0L, 0L)
  val edges = Array[Int](0, 2, 2, 0, 1)

  val tempDir = FileUtils.getTempDirectoryName
  val writer = new IntShardsWriter(tempDir, 10)
  writer.writeIntegersAtOffset(1, 0, List(2, 3))
  writer.writeIntegersAtOffset(2, 0, List(3, 4))
  writer.writeIntegersAtOffset(4, 0, List(1))
  writer.close

  def likeAnLRU = {

    "Work" in {
      l.get(1)(0) mustEqual 2
      l.get(1)(1) mustEqual 3
      l.getStats._1 mustEqual 1
      l.getStats._2 mustEqual 1
    }

    "Evict in LRU order" in {
      l2.get(1)
      l2.get(2)
      l2.get(1)
      l2.getStats._1 mustEqual 2
      l2.getStats._2 mustEqual 1
      l2.get(4)
      l2.get(1)
      l2.getStats._1 mustEqual 3
      l2.getStats._2 mustEqual 2
    }
  }

  val fastLRU = beforeContext {
    l = FastLRUIntArrayCache(Array(tempDir), 10, 4, 2, 3, offset, edges)
    l2 = FastLRUIntArrayCache(Array(tempDir), 10, 4, 2, 4, offset, edges)
  }

  val buffFastLRU = beforeContext {
    l = BufferedFastLRUIntArrayCache(Array(tempDir), 10, 4, 2, 3, offset, edges)
    l2 = BufferedFastLRUIntArrayCache(Array(tempDir), 10, 4, 2, 4, offset, edges)
  }

  val locklessFastLRU = beforeContext {
    l = LocklessReadFastLRUIntArrayCache(Array(tempDir), 10, 4, 2, 3, offset, edges)
    l2 = LocklessReadFastLRUIntArrayCache(Array(tempDir), 10, 4, 2, 4, offset, edges)
  }

  "FastLRUIntArrayCache" definedAs fastLRU should {
    "be like an LRU cache" in { likeAnLRU }
  }

  "BufferedFastLRUIntArrayCache" definedAs buffFastLRU should {
    "be like an LRU cache" in { likeAnLRU }
  }

  "LocklessReadFastLRUIntArrayCache" definedAs locklessFastLRU should {
    "be like an LRU cache" in { likeAnLRU }
  }

}
