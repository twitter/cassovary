/*
 * Copyright 2014 Twitter, Inc.
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

import org.scalatest.{Matchers, WordSpec}

class SortedArrayOpsSpec extends WordSpec with Matchers {
  "SortedArrayWrapper" should {
    "check if element exists in the array" when {
      "empty array" in {
        SortedArrayOps.exists(Array[Int](), 4) should be (false)
      }
      "array with 1 element" in {
        SortedArrayOps.exists(Array(1), 4) should be (false)
        SortedArrayOps.exists(Array(4), 4) should be (true)
      }
      "array with few elements" in {
        SortedArrayOps.exists(Array(1,2,3,4), 4) should be (true)
        SortedArrayOps.exists(Array(1,2,3,4), 5) should be (false)
      }
    }

    "compute an intersection of two arrays" when {
      "first of the arrays in empty" in {
        SortedArrayOps.intersectSorted(Array(), Array(1, 2, 3, 4)) should be (Array())
      }

      "second of the arrays in empty" in {
        SortedArrayOps.intersectSorted(Array(1, 2, 3, 4), Array()) should be (Array())
      }

      "arrays don't contain duplicates" in {
        SortedArrayOps.intersectSorted(Array(2, 3), Array(1, 4)) should be (Array())
      }

      "arrays contain duplicates" in {
        SortedArrayOps.intersectSorted(Array(2, 3), Array(1, 2, 3, 4)) should be (Array(2, 3))
      }
    }
    "compute a union of two arrays" when {
      "first is empty" in {
        SortedArrayOps.unionSorted(Array(), Array(1, 2, 3, 4)) should be (Array(1, 2, 3, 4))
      }
      "second is empty" in {
        SortedArrayOps.unionSorted(Array(1, 2, 3, 4), Array()) should be (Array(1, 2, 3, 4))
      }

      "arrays don't contain duplicates" in {
        SortedArrayOps.unionSorted(Array(2, 3), Array(1, 4)) should be (Array(1, 2, 3, 4))
      }

      "arrays contain duplicates" in {
        SortedArrayOps.unionSorted(Array(2, 3), Array(1, 2, 4)) should be (Array(1, 2, 3, 4))
      }
    }
  }
}
