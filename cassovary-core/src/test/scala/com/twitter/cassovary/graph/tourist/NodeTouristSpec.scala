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
package com.twitter.cassovary.graph.tourist

import com.twitter.cassovary.graph.util.FastUtilConversion
import com.twitter.cassovary.graph.{DirectedPath, TestNode}
import it.unimi.dsi.fastutil.ints.Int2IntMap
import it.unimi.dsi.fastutil.objects.Object2IntMap
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

class NodeTouristSpec extends WordSpec with ShouldMatchers {

  def testNode(id: Int) = TestNode(id, Nil, Nil)

  "VisitsCounter" should {
    "count visits properly" in {
      val visitor = new VisitsCounter
      List(1, 2, 3, 1, 2, 3, 1, 4, 2) foreach { id: Int =>
        visitor.visit(testNode(id))
      }

      visitMapToArray(visitor.infoAllNodes) shouldEqual Array((1, 3), (2, 3), (3, 2), (4, 1))
    }
  }

  "PathsCounter" should {
    "count paths properly with 2 home nodes" in {
      val visitor = new PathsCounter(10, List(1, 2))
      List(1, 2, 3, 4, 1, 2, 3, 4, 3, 1, 1, 4, 1, 3, 2, 3) foreach { id: Int =>
        visitor.visit(id)
      }
      val info = visitor.infoAllNodes

      pathMapToArray(info.get(1)) shouldEqual Array((DirectedPath(Array(1)), 5))
      pathMapToArray(info.get(2)) shouldEqual Array((DirectedPath(Array(2)), 3))
      pathMapToArray(info.get(3)) shouldEqual Array(
        (DirectedPath(Array(2, 3)), 3),
        (DirectedPath(Array(2, 3, 4, 3)), 1),
        (DirectedPath(Array(1, 3)), 1)
      )
      pathMapToArray(info.get(4)) shouldEqual Array(
        (DirectedPath(Array(2, 3, 4)), 2),
        (DirectedPath(Array(1, 4)), 1)
      )
    }
  }

  def pathMapToArray(map: Object2IntMap[DirectedPath]) = {
    FastUtilConversion.object2IntMapToArray(map)
  }

  def visitMapToArray(map: Int2IntMap) = {
    FastUtilConversion.int2IntMapToArray(map)
  }
}
