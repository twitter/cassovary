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

class SharedArraySeqSpec extends WordSpec with Matchers {
  val arr1 = Array(1,2,3)
  val arr2 = Array(4,5)

  "SharedArraySeq" when {
    "single shard" should {
      val sharedArray = Array[Array[Int]](arr1)

      "construct seqs correctly" in {
        val seq1 = new SharedArraySeq(0, sharedArray, 0, 3)
        seq1.toList shouldEqual arr1.toList
        new SharedArraySeq(0, sharedArray, 1, 2).toList shouldEqual List(2, 3)
        seq1(0) shouldEqual 1
        seq1(1) shouldEqual 2
        seq1(2) shouldEqual 3
        an [IndexOutOfBoundsException] should be thrownBy { seq1(3) }
      }

      "implement foreach correctly" in {
        (new SharedArraySeq(0, sharedArray, 0, 3) map { _ + 1 }) shouldEqual List(2, 3, 4)
      }
    }

    "multiple shard" should {
      val sharedArray = Array[Array[Int]](arr1, arr2)

      "construct seqs correctly" in {
        val seq1 = new SharedArraySeq(0, sharedArray, 0, 3)
        seq1.toList shouldEqual arr1.toList
        val seq2 = new SharedArraySeq(1, sharedArray, 0, 2)
        seq2.toList shouldEqual arr2.toList
        val seq3 = new SharedArraySeq(20, sharedArray, 0, 3)
        seq3.toList shouldEqual arr1.toList
        val seq4 = new SharedArraySeq(111, sharedArray, 0, 2)
        seq4.toList shouldEqual arr2.toList

        seq1(0) shouldEqual 1
        seq1(1) shouldEqual 2
        seq1(2) shouldEqual 3
        an [IndexOutOfBoundsException] should be thrownBy { seq1(3) }

        seq2(0) shouldEqual 4
        seq2(1) shouldEqual 5
        an [IndexOutOfBoundsException] should be thrownBy { seq2(2) }
      }

      "implement foreach correctly" in {
        (new SharedArraySeq(111, sharedArray, 0, 2) map { _ + 1 }) shouldEqual List(5, 6)
      }
    }
  }
}
