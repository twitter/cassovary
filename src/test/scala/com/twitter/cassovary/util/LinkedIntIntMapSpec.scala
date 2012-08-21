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

class LinkedIntIntMapSpec extends Specification {

  "LinkedIntIntMap" should {

    var l: LinkedIntIntMap = null
    doBefore {
      l = new LinkedIntIntMap(10, 5)
    }

    "Moving the head when there is only 1 element" in {
      l.addToHead(8)
      l.moveToHead(8)
      l.contains(8) mustBe true
      l.moveToHead(8)
      l.contains(8) mustBe true
      (1 to 10).filter(i => i != 8).foreach { i => l.contains(i) mustBe false }
    }

    "Reordering elements works" in {
      l.addToHead(5)
      l.addToHead(3)
      l.getHeadId mustEqual 3
      l.getTailId mustEqual 5
      l.moveToHead(5)
      l.moveToHead(6) must throwAn[IllegalArgumentException] // Can't move something that doesn't exist
      l.getHeadId mustEqual 5
      l.getTailId mustEqual 3
    }

    "Remove everything and add something" in {
      l.addToHead(10)
      l.removeFromTail()
      l.removeFromTail() must throwAn[IllegalArgumentException] // Cannot remove more than we have
      l.getCurrentSize mustEqual 0
      l.addToHead(9)
      l.contains(9) mustBe true
      (1 to 10).filter(i => i != 9).foreach { i => l.contains(i) mustBe false }
    }

    "Evict correctly when full" in {
      l.addToHead(5)
      l.addToHead(8)
      l.addToHead(1)
      l.addToHead(2)
      l.addToHead(10)
      l.addToHead(9) must throwAn[IllegalArgumentException] // Cannot exceed size of cache
      List(1, 2, 5, 8, 10).foreach { i => l.contains(i) mustBe true }
      List(3, 4, 6, 7, 9).foreach { i => l.contains(i) mustBe false }
      l.removeFromTail() // Evicts 5
      l.getTailId mustEqual 8
      l.removeFromTail() // Evicts 8
      l.getTailId mustEqual 1
      l.getCurrentSize mustEqual 3
      l.addToHead(9)
      l.addToHead(7)
      l.getCurrentSize mustEqual 5
      List(1, 2, 7, 9, 10).foreach { i => l.contains(i) mustBe true }
      List(3, 4, 5, 6, 8).foreach { i => l.contains(i) mustBe false }
    }

  }

}
