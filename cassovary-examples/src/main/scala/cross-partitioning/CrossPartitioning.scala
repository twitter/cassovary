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

import com.twitter.app.Flags
import com.twitter.cassovary.graph.distributed.{GraphFilesSplitter, HashSourceAndDestMapper}
import com.twitter.cassovary.util.io.AdjacencyListGraphReader

object CrossPartitioning extends App {

  val flags = new Flags("Cross Partitioning")
  val numInstances = flags("n", 10, "Number of instances/shards")
  val inputGraphDir = flags("i", "/tmp/input-graph", "Input graph directory")
  val subgraphsDir = flags("o", "/tmp/output-graph", "Output subgraphs directory")
  val helpFlag = flags("h", false, "Print usage")
  flags.parseArgs(args)

  val reader = AdjacencyListGraphReader.forIntIds(inputGraphDir(), "toy_6nodes_adj")

  def hashNodeFn(i: Int) = i

  val partitioner = new HashSourceAndDestMapper(numInstances(), hashNodeFn)
  val splitter = new GraphFilesSplitter[Int](subgraphsDir(), partitioner, reader)
  println(s"Now splitting graph in ${inputGraphDir()} into ${numInstances()} subgraphs.")
  splitter.splitGraph()
  println("Split is complete.")
}
