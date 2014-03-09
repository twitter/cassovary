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
package com.twitter.cassovary.graph

/**
 * A representation of the two directions edges point in a directed graph.
 */
object GraphDir extends Enumeration {
  type GraphDir = Value

  /**
   * Indicates an edge that points away from the current node and thus contributes to its
   * out-degree.
   */
  val OutDir = Value

  /**
   * Indicates an edge that points towards the current node and thus contributes to its in-degree.
   */
  val InDir = Value

  def reverse(other: GraphDir) = other match {
    case OutDir => InDir
    case InDir => OutDir
  }
}
