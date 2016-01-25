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
package com.twitter.cassovary.collections

import com.twitter.cassovary.collections.CSeq.Implicits._
import org.scalatest.{Matchers, WordSpec}

class SortedCSeqOpsSpec extends WordSpec with Matchers {
  "SortedCSeqOps" should {
    "check if element exists in the array" when {
      "empty array" in {
        SortedCSeqOps.exists(CSeq.empty[Int], 4) should be (false)
      }
      "array with 1 element" in {
        SortedCSeqOps.exists(CSeq(Array(1)), 4) should be (false)
        SortedCSeqOps.exists(CSeq(Array(4)), 4) should be (true)
      }
      "array with few elements" in {
        SortedCSeqOps.exists(CSeq(Array(1,2,3,4)), 4) should be (true)
        SortedCSeqOps.exists(CSeq(Array(1,2,3,4)), 5) should be (false)
      }
    }

    "compute an intersection of two CSeqs" when {
      "first of the arrays in empty" in {
        SortedCSeqOps.intersectSorted(CSeq.empty[Int], CSeq(Array(1, 2, 3, 4))) should be (CSeq.empty[Int])
      }

      "second of the arrays in empty" in {
        SortedCSeqOps.intersectSorted(CSeq(Array(1, 2, 3, 4)), CSeq.empty[Int]) should be (CSeq.empty[Int])
      }

      "arrays don't contain duplicates" in {
        SortedCSeqOps.intersectSorted(CSeq(Array(2, 3)), CSeq(Array(1, 4))) should be (CSeq.empty[Int])
      }

      "arrays contain duplicates" in {
        SortedCSeqOps.intersectSorted(CSeq(Array(2, 3)), CSeq(Array(1, 2, 3, 4))) should be (CSeq(Array(2, 3)))
      }
    }
    "compute a union of two CSeqs" when {
      "first is empty" in {
        SortedCSeqOps.unionSorted(CSeq.empty[Int], CSeq(Array(1, 2, 3, 4))) should be (CSeq(Array(1, 2, 3, 4)))
      }
      "second is empty" in {
        SortedCSeqOps.unionSorted(CSeq(Array(1, 2, 3, 4)), CSeq.empty[Int]) should be (CSeq(Array(1, 2, 3, 4)))
      }

      "arrays don't contain duplicates" in {
        SortedCSeqOps.unionSorted(CSeq(Array(2, 3)), CSeq(Array(1, 4))) should be (CSeq(Array(1, 2, 3, 4)))
      }

      "arrays contain duplicates" in {
        SortedCSeqOps.unionSorted(CSeq(Array(2, 3)), CSeq(Array(1, 2, 4))) should be (CSeq(Array(1, 2, 3, 4)))
      }
    }
  }
}
