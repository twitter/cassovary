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

import com.twitter.cassovary.graph.{DirectedPath, TestNode}
import com.twitter.cassovary.util.FastUtilUtils
import it.unimi.dsi.fastutil.ints.Int2IntMap
import it.unimi.dsi.fastutil.objects.Object2IntMap
import org.scalatest.{Matchers, WordSpec}

class NodeTouristSpec extends WordSpec with Matchers {

  def testNode(id: Int) = TestNode(id, Nil, Nil)

  "VisitsCounter" should {
    "count visits properly" in {
      val visitor = new VisitsCounter
      List(1, 2, 3, 1, 2, 3, 1, 4, 2) foreach { id: Int =>
        visitor.visit(testNode(id))
      }

      visitor.infoAllNodes.toSeq should be  (Array((1, 3), (2, 3), (3, 2), (4, 1)).toSeq)
    }
  }

  "PathsCounter" should {
    "count paths properly with 2 home nodes" in {
      val visitor = new PathsCounter(10, List(1, 2))
      List(1, 2, 3, 4, 1, 2, 3, 4, 3, 1, 1, 4, 1, 3, 2, 3) foreach { id: Int =>
        visitor.visit(id)
      }
      val info = visitor.infoAllNodes

      pathMapToSeq(info(1)) should be(Seq((DirectedPath(Array(1)), 5)))
      pathMapToSeq(info(2)) should be(Seq((DirectedPath(Array(2)), 3)))
      pathMapToSeq(info(3)) should be(Seq(
        (DirectedPath(Array(2, 3)), 3),
        (DirectedPath(Array(1, 3)), 1),
        (DirectedPath(Array(2, 3, 4, 3)), 1)
      ))
      pathMapToSeq(info(4)) should be(Seq(
        (DirectedPath(Array(2, 3, 4)), 2),
        (DirectedPath(Array(1, 4)), 1)
      ))
    }
  }

  "PrevNbrCounter" should {
    "store all previous neighbors" in {
      val prevNbrCounter = new PrevNbrCounter(Some(2), false)
      prevNbrCounter.recordPreviousNeighbor(5, 4)
      prevNbrCounter.recordPreviousNeighbor(5, 2)
      prevNbrCounter.recordPreviousNeighbor(5, 1)
      prevNbrCounter.recordPreviousNeighbor(5, 3)
      prevNbrCounter.recordPreviousNeighbor(5, 1)
      prevNbrCounter.recordPreviousNeighbor(5, 4)

      prevNbrCounter.infoOfNode(5).map(FastUtilUtils.int2IntMapToMap) should be
        Some(Map(4 -> 2, 1 -> 2, 3 -> 1, 2 -> 1))
    }

    "store top 2 previous neighbors" in {
      val prevNbrCounter = new PrevNbrCounter(None, false)
      prevNbrCounter.recordPreviousNeighbor(5, 4)
      prevNbrCounter.recordPreviousNeighbor(5, 2)
      prevNbrCounter.recordPreviousNeighbor(5, 1)
      prevNbrCounter.recordPreviousNeighbor(5, 3)
      prevNbrCounter.recordPreviousNeighbor(5, 1)
      prevNbrCounter.recordPreviousNeighbor(5, 4)

      prevNbrCounter.infoOfNode(5).map(FastUtilUtils.int2IntMapToMap) should be
        Some(Map(4 -> 2, 1 -> 2))
    }
  }

  def pathMapToSeq(map: Object2IntMap[DirectedPath]) = {
    FastUtilUtils.object2IntMapToArray(map).toSeq
  }

  def visitMapToSeq(map: Int2IntMap) = {
    FastUtilUtils.int2IntMapToArray(map).toSeq
  }
}
