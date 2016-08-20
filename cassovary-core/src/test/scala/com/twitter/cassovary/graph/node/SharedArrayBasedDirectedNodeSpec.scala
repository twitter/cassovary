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
package com.twitter.cassovary.graph.node

import com.twitter.cassovary.graph.StoredGraphDir
import com.twitter.cassovary.util.Sharded2dArray
import com.twitter.cassovary.collections.CSeq

import com.twitter.cassovary.collections.CSeq.Implicits._

class SharedArrayBasedDirectedNodeSpec extends NodeBehaviors {
  val nodesIndicator: Int => Boolean = {case `nodeId` => true; case _ => false}
  val offsets: Int => Int = {case `nodeId` => 0; case _ => -1}
  val lengthsOut: Int => Int = {case `nodeId` => 3; case _ => -1}
  val lengthsIn: Int => Int = {case `nodeId` => 2; case _ => -1}

  val sharedOutEdgesArray = new Sharded2dArray[Int](Array[Array[Int]](neighbors),
    nodesIndicator, offsets, lengthsOut, (x: Int) => 0)
  val sharedInEdgesArray = new Sharded2dArray[Int](Array[Array[Int]](inEdges),
    nodesIndicator, offsets, lengthsIn, (x: Int) => 0)

  val actualIn = SharedArrayBasedDirectedNode(nodeId, sharedOutEdgesArray,
          StoredGraphDir.OnlyIn)
  val actualOut = SharedArrayBasedDirectedNode(nodeId, sharedOutEdgesArray,
          StoredGraphDir.OnlyOut)
  val actualMutual = SharedArrayBasedDirectedNode(nodeId, sharedOutEdgesArray,
          StoredGraphDir.Mutual)
  val actualBoth = SharedArrayBasedDirectedNode(nodeId, sharedOutEdgesArray,
          StoredGraphDir.BothInOut, Some(sharedInEdgesArray))
  correctlyConstructNodes(actualIn, actualOut, actualMutual, actualBoth)
}
