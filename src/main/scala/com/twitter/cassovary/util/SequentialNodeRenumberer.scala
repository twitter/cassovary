/*
 * Copyright 2013 Twitter, Inc.
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

import scala.collection.mutable.HashMap

/*
 * Renumbers node id values read from source file to internal sequentially increasing id value.
   Uses a synchronized map for thread safety when shared by multiple reader threads.
 */
class SequentialNodeRenumberer extends NodeRenumberer {

  val nodeIdToNodeIdxMap = HashMap[Int,Int]()
  var lazyNodeIdxToNodeIdMap = Array[Int]()

  def externalToInternal(externalNodeId: Int): Int =
    this.synchronized {
      nodeIdToNodeIdxMap.getOrElseUpdate(externalNodeId, nodeIdToNodeIdxMap.size)
    }

  def internalToExternal(internalNodeId: Int): Int = {
    if (lazyNodeIdxToNodeIdMap.isEmpty) {
      lazyNodeIdxToNodeIdMap = new Array[Int](nodeIdToNodeIdxMap.size)
      nodeIdToNodeIdxMap.foreach { case (externalNodeId, internalNodeId) => lazyNodeIdxToNodeIdMap(internalNodeId) = externalNodeId }
    }
    lazyNodeIdxToNodeIdMap(internalNodeId)
  }

}
