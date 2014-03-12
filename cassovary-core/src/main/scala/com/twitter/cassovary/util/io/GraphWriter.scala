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
package com.twitter.cassovary.util.io

import com.twitter.cassovary.graph._
import java.io.{PrintWriter,Writer}

/**
 * Utility class for writing a graph object to a Writer output stream,
 * such that it could be read back in by a GraphReader.
 */
object GraphWriter {

  /**
   * Writes given graph to given writer by iterating over all nodes and outbound edges.
   */
  def writeDirectedGraph(graph: DirectedGraph, writer: Writer) = {
    val gWriter = new PrintWriter(writer)
    graph.foreach { v => {
        gWriter.println(v.id + " " + v.outboundCount)
        v.outboundNodes().foreach { ngh => 
          gWriter.println(ngh)
        }
      }
    }
    gWriter.close()
  }

}
