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

import com.twitter.cassovary.graph.NodeIdEdgesMaxId
import com.twitter.cassovary.util._
import com.twitter.ostrich.stats.Stats
import java.util.concurrent.{Future, Executors}
import net.lag.logging.Logger
import util.control.Exception
import java.io.File

object RenumberGraphDirectory {

  private case class MaxIdsEdges(localMaxId:Int, localNodeWithoutOutEdgesMaxId:Int, numEdges:Int, nodeCount:Int)

  private lazy val log = Logger.get("RenumberGraphDirectory")

  def main(args: Array[String]) {

    if (args.length < 3) {
      throw new Exception ("Provide source graphdir, destination and renumber mapping file!")
    }

    val numShards = 16
    val outDirectory = args(1)
    val renumberFile = args(2)

    val iteratorSeq = new GraphLoader.RawEdgeShardsReader(args(0)).readers
    val executorService = Executors.newFixedThreadPool(8)

    // Load graph once to get maxId
    println("Loading graph to determine maxId...")
    var maxId = 0
    var nodeWithOutEdgesMaxId = 0
    var numEdges = 0
    var nodeWithOutEdgesCount = 0
    val futures1 = Stats.time("graph_load_reading_maxid_and_calculating_numedges") {
      def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) = {
        var localMaxId, localNodeWithOutEdgesMaxId, numEdges, nodeCount = 0
        iteratorFunc().foreach { item =>
        // Keep track of Max IDs
          localMaxId = localMaxId max item.maxId
          localNodeWithOutEdgesMaxId = localNodeWithOutEdgesMaxId max item.id
          // Update nodeCount and total edges
          numEdges += item.edges.length
          nodeCount += 1
        }
        MaxIdsEdges(localMaxId, localNodeWithOutEdgesMaxId, numEdges, nodeCount)
      }
      ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], MaxIdsEdges](executorService,
        iteratorSeq, readOutEdges)
    }
    futures1.toArray map { future =>
      val f = future.asInstanceOf[Future[MaxIdsEdges]]
      val MaxIdsEdges(localMaxId, localNWOEMaxId, localNumEdges, localNodeCount) = f.get
      maxId = maxId max localMaxId
      nodeWithOutEdgesMaxId = nodeWithOutEdgesMaxId max localNWOEMaxId
      numEdges += localNumEdges
      nodeWithOutEdgesCount += localNodeCount
    }

    // Load graph a second time to map only the out-nodes
    println("Loading graph again to map only out-nodes...")
    val ren = new Renumberer(maxId)
    val futures2 = Stats.time("graph_load_renumbering_nodes_with_outedges") {
      def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) = {
        iteratorFunc().foreach { item =>
          ren.translate(item.id)
        }
      }
      ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
        iteratorSeq, readOutEdges)
    }
    futures2.toArray map { future =>
      future.asInstanceOf[Future[Unit]].get
    }
    assert(ren.count == nodeWithOutEdgesCount) // Sanity check

    // Load graph a final time to map the edges and write them out
    println("Loading graph to write out the renumbered version...")
    new File(outDirectory).mkdirs()
    val files = (0 until numShards).map { i =>
      FileUtils.printWriter("%s/part-r-%05d".format(outDirectory, i))
    }
    val futures3 = Stats.time("graph_load_write_nodes_with_outedges") {
      def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) = {
        iteratorFunc().foreach { item =>
          val id = ren.translate(item.id)
          val fileShard = files(id % numShards)
          val edgeIds = ren.translateArray(item.edges)
          fileShard.println("%s\t%s".format(id, item.edges.length))
          edgeIds.foreach { eid => fileShard.println(eid) }
        }
      }
      ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
        iteratorSeq, readOutEdges)
    }
    // Make sure everything finishes
    futures3.toArray map { future => future.asInstanceOf[Future[Unit]].get }
    // Close all files
    files.foreach { f => f.close() }

    // Write mapping out
    println("Writing mapping out...")
    val serializer = new LoaderSerializerWriter(renumberFile)
    ren.toWriter(serializer)
    serializer.close

    println("Done!")
  }

}