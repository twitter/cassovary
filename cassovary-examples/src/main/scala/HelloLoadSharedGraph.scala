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

import com.twitter.cassovary.graph.StoredGraphDir
import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir
import com.twitter.cassovary.util.NodeNumberer
import com.twitter.cassovary.util.io.AdjacencyListGraphReader

object HelloLoadSharedGraph {
  def main(args: Array[String]) {
    val dir = "../cassovary-core/src/test/resources/graphs"
    val graph = AdjacencyListGraphReader.forIntIds(dir, "toy_9nodes").toSharedArrayBasedDirectedGraph()

    printf("\nHello Graph!\n\tA SharedArrayBasedDirectedGraph:" +
        "with %s nodes has %s directed edges.\n", graph.nodeCount, graph.edgeCount)

    printf("Trying with BothInOut direction now:\n")
    val reader = new AdjacencyListGraphReader(
          dir, "toy_9nodes", new NodeNumberer.IntIdentity(), _.toInt) {
          override def storedGraphDir: StoredGraphDir = StoredGraphDir.BothInOut
    }
    val graph2 = reader.toSharedArrayBasedDirectedGraph()
    printf("\nHello Graph!\n\tA SharedArrayBasedDirectedGraph:" +
        "with %s nodes has %s directed edges.\n", graph2.nodeCount, graph2.edgeCount)

  }
}
