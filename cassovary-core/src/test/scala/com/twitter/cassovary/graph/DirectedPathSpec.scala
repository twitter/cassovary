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
package com.twitter.cassovary.graph

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

class DirectedPathSpec extends WordSpec with ShouldMatchers  {

  "path of many nodes" should {
    "length, append, exists, equals" in {
      val testPathIds = Array(10, 11, 12, 14, 15, 11, 14, 0)
      val path = DirectedPath.builder()
      path.append(testPathIds(0))
      path.snapshot shouldEqual DirectedPath(Array(testPathIds(0)))
      (1 until testPathIds.length - 2) foreach { indx =>
        val node = testPathIds(indx)
        path.append(node)
        val snapshotted = path.snapshot
        snapshotted.length shouldEqual (indx + 1)
        snapshotted.exists(node) shouldEqual true
        snapshotted.exists(testPathIds(indx - 1)) shouldEqual true
        snapshotted.exists(testPathIds(testPathIds.length - 1)) shouldEqual false
      }

      val path2 = DirectedPath.builder()
      path2.append(testPathIds(0))
      (path.snapshot == path2.snapshot) shouldEqual false

      path.clear()
      path.snapshot.length shouldEqual 0
      path.append(testPathIds(0)).append(testPathIds(1))
      path.snapshot.length shouldEqual 2

      path2.clear()
      path2.append(testPathIds(0))
      path2.append(testPathIds(1))
      path2.snapshot shouldEqual DirectedPath(Array(testPathIds(0), testPathIds(1)))
      (path.snapshot == path2.snapshot) shouldEqual true
    }
  }
}
