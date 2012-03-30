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
package com.twitter.cassovary.graph.tourist

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap

/**
 * A tourist that keeps counts of the number of times a node has been seen.
 */
class VisitsCounter extends NodeTourist with InfoKeeper[Int] {

  override protected val infoPerNode = new Int2IntOpenHashMap

  def visit(id: Int) {
    infoPerNode.add(id, 1)
  }

  override def infoAllNodes: Array[(Int, Int)] = {
    val info = super.infoAllNodes
    info.sortWith { (node, count) => (-count, node) }
  }

}