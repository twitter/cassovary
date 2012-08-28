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

class RandomizedIntArrayCacheSpec extends Specification {

  var ri: IntArrayCache = _
  var ri2: IntArrayCache = _
  val offset = Array[Long](0L, 0L, 0L, 0L, 0L)
  val edges = Array[Int](0, 2, 2, 0, 1)

  val tempDir = FileUtils.getTempDirectoryName
  val writer = new IntShardsWriter(tempDir, 10)
  writer.writeIntegersAtOffset(1, 0, List(2, 3))
  writer.writeIntegersAtOffset(2, 0, List(3, 4))
  writer.writeIntegersAtOffset(4, 0, List(1))
  writer.close

  def likeARandomizedCache {
    "Generally work" in {
      ri.get(1)(0) mustEqual 2
      ri.get(1)(1) mustEqual 3
      val (rim, rih, _, _) = ri.getStats
      rim mustEqual 1
      rih mustEqual 1
    }

    "Successfully evict nodes" in {
      ri2.get(1)
      ri2.get(2)
      ri2.get(1)
      ri2.get(2)
      val (rim, rih, _, _) = ri2.getStats
      rim must beGreaterThan(2L)
      rih must beLessThan(2L)
    }
  }

  def randomizedCache = beforeContext {
    ri = RandomizedIntArrayCache(Array(tempDir), 10, 4, 2, 3, offset, edges)
    ri2 = RandomizedIntArrayCache(Array(tempDir), 10, 4, 2, 3, offset, edges)
  }

  def locklessRandomizedCache = beforeContext {
    ri = LocklessRandomizedIntArrayCache(Array(tempDir), 10, 4, 2, 3, offset, edges)
    ri2 = LocklessRandomizedIntArrayCache(Array(tempDir), 10, 4, 2, 3, offset, edges)
  }

  "RandomizedIntArrayCache" definedAs randomizedCache should {
    "be like a randomized cache" in { likeARandomizedCache }
  }

  "LocklessRandomizedIntArrayCache" definedAs locklessRandomizedCache should {
    "be like a randomized cache" in { likeARandomizedCache }
  }

}
