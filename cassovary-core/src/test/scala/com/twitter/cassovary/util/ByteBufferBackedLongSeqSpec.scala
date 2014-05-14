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

import java.nio.ByteBuffer
import org.specs.Specification

class ByteBufferBackedLongSeqSpec extends Specification {

  def bbForList(ll: Seq[Long]): ByteBuffer = {
    val bb = ByteBuffer.allocate(ll.size * 8)
    ll foreach { l => bb.putLong(l) }
    bb.flip()
    bb
  }

  "A ByteBufferBackedLongSeq" should {
    "Get the same list back" in {
      val testSeq = Seq(12L, 3L, 5L, 56L)
      val bb = bbForList(testSeq)
      new ByteBufferBackedLongSeq(bb) mustEqual testSeq
    }
  }
}
