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
package com.twitter.cassovary.graph

import com.twitter.cassovary.util.{NodeRenumberer,SequentialNodeRenumberer,SharedArraySeq}
import org.specs.Specification

class SharedArraySeqSpec extends Specification {
  val arr1 = Array(1,2,3)
  val arr2 = Array(4,5)
  var sharedArray: Array[Array[Int]] = _

  val singleShard = beforeContext {
    sharedArray = Array[Array[Int]](arr1)
  }

  val multiShard = beforeContext {
    sharedArray = Array[Array[Int]](arr1, arr2)
  }

  "single shard SharedArraySeq" definedAs singleShard should {

    "constructs seqs correctly" in {
      val seq1 = new SharedArraySeq(0, sharedArray, 0, 3)
      seq1.toList mustEqual arr1.toList
      (new SharedArraySeq(0, sharedArray, 1, 2)).toList mustEqual List(2, 3)
      seq1(0) mustEqual 1
      seq1(1) mustEqual 2
      seq1(2) mustEqual 3
      seq1(3) must throwA[IndexOutOfBoundsException]
    }

    "implements foreach correctly" in {
      ((new SharedArraySeq(0, sharedArray, 0, 3)) map { _ + 1 }) mustEqual List(2, 3, 4)
    }
  }

  "multiple shard SharedArraySeq" definedAs multiShard should {

    "constructs seqs correctly" in {
      val seq1 = new SharedArraySeq(0, sharedArray, 0, 3)
      seq1.toList mustEqual arr1.toList
      val seq2 = new SharedArraySeq(1, sharedArray, 0, 2)
      seq2.toList mustEqual arr2.toList
      val seq3 = new SharedArraySeq(20, sharedArray, 0, 3)
      seq3.toList mustEqual arr1.toList
      val seq4 = new SharedArraySeq(111, sharedArray, 0, 2)
      seq4.toList mustEqual arr2.toList

      seq1(0) mustEqual 1
      seq1(1) mustEqual 2
      seq1(2) mustEqual 3
      seq1(3) must throwA[IndexOutOfBoundsException]

      seq2(0) mustEqual 4
      seq2(1) mustEqual 5
      seq2(2) must throwA[IndexOutOfBoundsException]
    }

    "implements foreach correctly" in {
      ((new SharedArraySeq(111, sharedArray, 0, 2)) map { _ + 1 }) mustEqual List(5, 6)
    }
  }
}
