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

class ArrayBasedDirectedNodeSpec extends NodeBehaviors {
  val actualIn = ArrayBasedDirectedNode(nodeId, neighbors, StoredGraphDir.OnlyIn)
  val actualOut = ArrayBasedDirectedNode(nodeId, neighbors, StoredGraphDir.OnlyOut)
  val actualMutual = ArrayBasedDirectedNode(nodeId, neighbors, StoredGraphDir.Mutual)
  val actualBoth = ArrayBasedDirectedNode(nodeId, neighbors, StoredGraphDir.BothInOut)
  actualBoth.asInstanceOf[BiDirectionalNode].inEdges = inEdges
  correctlyConstructNodes(actualIn, actualOut, actualMutual, actualBoth)
}
