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

import org.specs.Specification
import java.nio.file.Files
import java.nio.ByteBuffer

class ConstantDBSpec extends Specification {
  val edges = Seq(
    "a" -> "abc",
    "bdg" -> "efg"
  )

  val tempDir = Files.createTempDirectory("cassovary").toString

  ConstantDB.write(
    tempDir,
    edges.toStream map { case (k, v) =>
      ByteBuffer.wrap(k.getBytes) -> ByteBuffer.wrap(v.getBytes)
    },
    2080
    )

  val cdb = new ConstantDB(
    tempDir,
    (x: String) => ByteBuffer.wrap(x.getBytes),
    (x: ByteBuffer) =>{
      val b = new Array[Byte](x.remaining())
      x.get(b)
      new String(b)
    },
    (x: ByteBuffer) => {
      val b = new Array[Byte](x.remaining())
      x.get(b)
      new String(b)
    }
    )

  "A ConstantDB" should {
    "number of parts should be correct" in {
      cdb.numParts mustEqual 2
    }

    "retrive data correctly" in {
      cdb("a") mustEqual Some("abc")
      cdb("bdg") mustEqual Some("efg")
      cdb("0") mustEqual None
      cdb("asdf") mustEqual None
      cdb("z") mustEqual None
    }

    "throw exception if it finds an item which cannot fit into a single file" in {
      val tempDir2 = Files.createTempDirectory("cassovary").toString

      ConstantDB.write(
        tempDir2,
        edges.toStream map { case (k, v) =>
          ByteBuffer.wrap(k.getBytes) -> ByteBuffer.wrap(v.getBytes)
        },
        2076
      ) must throwA[Exception]
    }
  }
}
