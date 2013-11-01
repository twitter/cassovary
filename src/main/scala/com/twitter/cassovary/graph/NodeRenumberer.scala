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

import scala.collection.mutable.{SynchronizedMap,HashMap}

/*
 * Handles renumbering each nodeId read from source file into a nodeIdx.
 */
trait NodeRenumberer {

  def nodeIdToNodeIdx(nodeId: Int): Int

  def nodeIdxToNodeId(nodeIdx: Int): Int

}


/*
 * Performs no node renumbering: simply echoes the passed id or idx to the caller.
 */
class IdentityNodeRenumberer extends NodeRenumberer {
  
  def nodeIdToNodeIdx(nodeId: Int): Int = {
    nodeId
  }

  def nodeIdxToNodeId(nodeIdx: Int): Int = {
    nodeIdx
  }

}


/*
 * Renumbers read ids sequentially.
   Uses a synchronized map for thread safety when shared by multiple reader threads.
 */
class SequentialNodeRenumberer extends NodeRenumberer {

  val nodeIdToNodeIdxMap = new HashMap[Int,Int] with SynchronizedMap[Int,Int]
  var lazyNodeIdxToNodeIdMap: Array[Int] = Array[Int]()

  def nodeIdToNodeIdx(nodeId: Int): Int = {
    nodeIdToNodeIdxMap.getOrElseUpdate(nodeId, nodeIdToNodeIdxMap.size)
  }

  def nodeIdxToNodeId(nodeIdx: Int): Int = {
    if (lazyNodeIdxToNodeIdMap.isEmpty) {
      lazyNodeIdxToNodeIdMap = new Array[Int](nodeIdToNodeIdxMap.size)
      nodeIdToNodeIdxMap.foreach { case (nodeId, nodeIdx) => lazyNodeIdxToNodeIdMap(nodeIdx) = nodeId }
    }
    lazyNodeIdxToNodeIdMap(nodeIdx)
  }

}
