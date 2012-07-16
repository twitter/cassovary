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
import com.twitter.cassovary.graph.{GraphDir, Node, TestGraphs}
import scala.io.Source
import java.io.File
import com.twitter.io.Files

class ArrayBasedLRUCacheSpec extends Specification {
  "ArrayBasedLRUCache" should {
    var cache: ArrayBasedLRUCache[Node] = null
    val graph = TestGraphs.g6
    val n10 = graph.getNodeById(10).get // 3
    val n11 = graph.getNodeById(11).get // 2
    val n12 = graph.getNodeById(12).get // 1
    val n13 = graph.getNodeById(13).get // 2
    val n14 = graph.getNodeById(14).get // 1
    val n15 = graph.getNodeById(15).get // 2

    doBefore {
      cache = new ArrayBasedLRUCache[Node](4, 8, 20)
    }

    "be able to add a single node" in {
      cache.addToHead(11, n11)
      cache.currIndexCapacity = 1
      cache.currRealCapacity mustEqual n11.neighborCount(GraphDir.OutDir)
      cache.contains(11) mustEqual true
      cache.get(11) mustEqual n11
      cache.contains(0) mustEqual false
    }

    "must evict in LRU order" in {
      cache.addToHead(11, n11)
      cache.addToHead(10, n10)
      cache.addToHead(13, n13)
      cache.addToHead(15, n15)
      cache.contains(10) mustEqual true
      cache.contains(11) mustEqual false
      cache.contains(13) mustEqual true
      cache.contains(14) mustEqual false
      cache.contains(15) mustEqual true
    }

    "must update the order of the list and obey real capacity" in {
      cache.addToHead(14, n14)
      cache.addToHead(13, n13)
      cache.addToHead(12, n12)
      cache.addToHead(10, n10)
      cache.moveToHead(14)
      cache.addToHead(15, n15) // bye 13
      cache.contains(10) mustEqual true
      cache.contains(11) mustEqual false
      cache.contains(12) mustEqual true
      cache.contains(13) mustEqual false
      cache.contains(14) mustEqual true
      cache.contains(15) mustEqual true
      cache.moveToHead(10)
      cache.addToHead(13, n13) // bye 12
      cache.contains(10) mustEqual true
      cache.contains(11) mustEqual false
      cache.contains(12) mustEqual false
      cache.contains(13) mustEqual true
      cache.contains(14) mustEqual true
      cache.contains(15) mustEqual true
      cache.currIndexCapacity mustEqual 4
      cache.currRealCapacity mustEqual 8
      cache.addToHead(11, n11) // bye 14, 15
      cache.contains(10) mustEqual true
      cache.contains(11) mustEqual true
      cache.contains(12) mustEqual false
      cache.contains(13) mustEqual true
      cache.contains(14) mustEqual false
      cache.contains(15) mustEqual false
      cache.currIndexCapacity mustEqual 3
      cache.currRealCapacity mustEqual 7
    }

    "must obey index capacity" in {
      val cache = new ArrayBasedLRUCache[Node](4, 100, 20)
      cache.addToHead(14, n14)
      cache.addToHead(13, n13)
      cache.addToHead(12, n12)
      cache.addToHead(10, n10)
      cache.moveToHead(14)
      cache.addToHead(15, n15) // bye 13
      cache.contains(10) mustEqual true
      cache.contains(11) mustEqual false
      cache.contains(12) mustEqual true
      cache.contains(13) mustEqual false
      cache.contains(14) mustEqual true
      cache.contains(15) mustEqual true
      cache.currIndexCapacity mustEqual 4
    }
  }
}
