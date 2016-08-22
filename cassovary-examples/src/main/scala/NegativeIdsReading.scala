/*
 * Copyright 2016 Twitter, Inc.
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
import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.util.SequentialNodeNumberer
import com.twitter.cassovary.util.io.ListOfEdgesGraphReader

/**
  * This example shows how to read a graph that has negative ids.
  *
  * Note that Cassovary is internally optimized for ids that are consecutive, positive
  * integers. Thus, we need to use a NodeNumberer (that is a bi-directional map) to
  * encode external ids to internal and a custom parser to parse negative numbers.
  */
object NegativeIdsReading {
  val GRAPHS_DIRECTORY = "src/main/resources/graphs"

  def main(args: Array[String]) = {
    def parser(s: String, beg: Int, end: Int): Int = {
      s.substring(beg, end + 1).toInt
    }

    val reader = new ListOfEdgesGraphReader[Int](GRAPHS_DIRECTORY, "negative-ids-cycle4",
      nodeNumberer = new SequentialNodeNumberer(), parser) {
      override def storedGraphDir: StoredGraphDir = StoredGraphDir.OnlyOut
    }

    val graph = reader.toSharedArrayBasedDirectedGraph()

    print(graph.toString())
  }
}
