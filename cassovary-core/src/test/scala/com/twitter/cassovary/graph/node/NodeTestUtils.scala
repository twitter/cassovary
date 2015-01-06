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

package com.twitter.cassovary.graph.node

import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.graph.{Node, StoredGraphDir}
import org.scalatest.matchers.{MatchResult, Matcher}

object NodeTestUtils {

  def deepEquals(expected: Node) = new Matcher[Node] {
    def apply(left: Node) = MatchResult(
      (expected.id == left.id) && (expected.outboundNodes.toList == left.outboundNodes.toList) &&
          (expected.inboundNodes.toList == left.inboundNodes.toList),
      "Nodes did not match. Expected: %s Actual: %s".format(expected, left),
      "Nodes are equal"
    )
  }
}
