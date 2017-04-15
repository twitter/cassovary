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

import scala.collection.mutable
import scala.io.Source
import StoredGraphDir.StoredGraphDir

object ReadFromEdgelist {
  def arrayBasedDirectedGraph(edgelistPath: String, dir: StoredGraphDir): ArrayBasedDirectedGraph = {
    val adjList = mutable.Map[Int, mutable.Set[Int]]()

    for (line <- Source.fromFile(edgelistPath).getLines) {
      val linesplit = line.split("\\s+")
      val u = linesplit(0).toInt
      val v = linesplit(1).toInt

      if (adjList.contains(u)) adjList(u).add(v)
      else adjList(u) = mutable.Set[Int](v)
    }

    val nodes = adjList.map(pair => NodeIdEdgesMaxId(pair._1, pair._2.toArray))
    ArrayBasedDirectedGraph(() => nodes.iterator, dir)
  }
}