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
package com.twitter.cassovary.graph.node

import com.twitter.cassovary.graph.StoredGraphDir
import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.graph.{NodeIdEdgesMaxIdTrait,LabeledNodeIdEdgesMaxId}

object ArrayBasedDirectedNode {
  val noNodes = Array.empty[Int]
  /**
   * creates an array based directed node (uni-directional or bi-directional)
   * @param nodeId an id of the node
   * @param neighbors a seq of ids of the neighbors read from file
   * @param dir the stored graph direction (OnlyIn, OnlyOut, BothInOut or Mutual)
   *
   * @return a node
   */
  def apply(nodeAndEdges: NodeIdEdgesMaxIdTrait, dir: StoredGraphDir) = {
    dir match {
      case StoredGraphDir.OnlyIn | StoredGraphDir.OnlyOut | StoredGraphDir.Mutual =>
        if (nodeAndEdges.isLabeled) {
          UniDirectionalLabeledNode(nodeAndEdges.id, nodeAndEdges.asInstanceOf[LabeledNodeIdEdgesMaxId].label, nodeAndEdges.edges, dir)
        } else {
          UniDirectionalNode(nodeAndEdges.id, nodeAndEdges.edges, dir)
        }
      case StoredGraphDir.BothInOut =>
        if (nodeAndEdges.isLabeled) {
          BiDirectionalLabeledNode(nodeAndEdges.id, nodeAndEdges.asInstanceOf[LabeledNodeIdEdgesMaxId].label, nodeAndEdges.edges)
        } else {
          BiDirectionalNode(nodeAndEdges.id, nodeAndEdges.edges)
        }
    }
  }
}
