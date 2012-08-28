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
import java.io.File

class RenumbererSpec extends Specification  {

  "Renumberer" should {

    var r: Renumberer = null
    doBefore {
      r = new Renumberer(100)
    }

    "Renumber forwards correctly" in {
      r.translate(100) mustEqual 1
      r.translate(0) mustEqual 2
      r.translate(15) mustEqual 3
      r.translate(100) mustEqual 1 // Getting it again returns the same index
      r.translate(16) mustEqual 4
    }

    "Renumber backwards correctly" in {
      r.translate(100) // -> 1
      r.translate(8) // -> 2
      r.translate(2) // -> 3
      r.reverseTranslate(200) mustEqual 0 // Exceeds range
      r.reverseTranslate(15) mustEqual 0 // In range but undefined
      r.reverseTranslate(1) mustEqual 100
      r.reverseTranslate(3) mustEqual 2
      r.translate(4) // Work even after a new translation
      r.reverseTranslate(2) mustEqual 8
      r.reverseTranslate(4) mustEqual 4
    }

    "Successfully load from disk" in {
      val tmpFile = FileUtils.getTempFilename
      r.translate(99)
      r.translate(100)
      r.translate(1)
      val lsw = new LoaderSerializerWriter(tmpFile)
      r.toWriter(lsw)
      lsw.close
      val s = new Renumberer(0)
      val lsr = new LoaderSerializerReader(tmpFile)
      s.fromReader(lsr)
      lsr.close
      s.translate(1) mustEqual 3
      s.translate(99) mustEqual 1
      s.translate(100) mustEqual 2
      s.reverseTranslate(3) mustEqual 1
    }

  }

}
