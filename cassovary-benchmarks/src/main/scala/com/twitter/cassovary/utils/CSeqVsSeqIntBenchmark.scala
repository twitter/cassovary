/*
 * Copyright 2015 Twitter, Inc.
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
package com.twitter.cassovary.utils


import com.twitter.cassovary.collections.CSeq
import org.openjdk.jmh.annotations.{Benchmark, Scope, Setup, State}
import CSeq.Implicits._

@State(Scope.Thread)
class CSeqVsSeqIntBenchmark {
  val seqsLength = 100000

  var seq: Seq[Int] = _

  var cSeq: CSeq[Int] = _

  @Setup
  def prepareSeqs() = {
    val arr = Array.tabulate(seqsLength)(n => n)
    seq = arr.toSeq
    cSeq = CSeq(arr)
  }


  @Benchmark
  def sumOfCSeq(): Int = {
    var s = 0
    cSeq.foreach(e => s += e)
    s
  }

  @Benchmark
  def sumOfSeq(): Int = {
    seq.sum
  }
}
