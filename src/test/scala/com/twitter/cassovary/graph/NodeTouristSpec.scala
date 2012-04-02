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

import com.twitter.cassovary.graph.tourist.{VisitsCounter, PathsCounter}
import org.specs.Specification

class NodeTouristSpec extends Specification {

  def testNode(id: Int) = TestNode(id, Nil, Nil)

  "visitscounter" should {
    "count visits properly" in {
      val visitor = new VisitsCounter
      List(1, 2, 3, 1, 2, 3, 1, 4, 2) foreach { id: Int =>
        visitor.visit(testNode(id))
      }
      val info = visitor.infoAllNodes
      info.get(1) mustEqual 3
      info.get(2) mustEqual 3
      info.get(3) mustEqual 2
      info.get(4) mustEqual 1
      info.containsKey(5) mustEqual false
    }
  }

  "pathscounter" should {
    "count paths properly with one homenode" in {
      val visitor = new PathsCounter(10, List(1, 2))
      List(1, 2, 3, 4, 1, 2, 3, 4, 3, 1, 1, 4, 1, 3, 2, 3) foreach { id: Int =>
        visitor.visit(id)
      }
      val info = visitor.infoAllNodes

      info.get(1).toSeq mustEqual Array(DirectedPath(Array(1))).toSeq
      info.get(2).toSeq mustEqual Array(DirectedPath(Array(2))).toSeq
      info.get(3).toSeq mustEqual Array(
        DirectedPath(Array(2, 3)),
        DirectedPath(Array(2, 3, 4, 3)),
        DirectedPath(Array(1, 3))
      ).toSeq
      info.get(4).toSeq mustEqual Array(
        DirectedPath(Array(2, 3, 4)),
        DirectedPath(Array(1, 4))
      ).toSeq
    }
  }
}
