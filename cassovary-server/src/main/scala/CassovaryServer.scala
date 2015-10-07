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

import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.graph.{TestGraphs, DirectedGraph, GraphUtils, Node}
import com.twitter.finagle.{Httpx, Service}
import com.twitter.finagle.httpx.{Request, Response, Status}
import com.twitter.io.Charsets.Utf8
import com.twitter.logging.Logger
import com.twitter.server.TwitterServer
import com.twitter.util.{Await, Future}
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import org.jboss.netty.handler.codec.http._

/*
 * A simple Http server demonstrating the use of Finagle-Stats library.
 * The server runs an admin service on localhost:9990 and another
 * service on localhost:8888 that responds every Http request by
 * a walk on a generated graph.
 */
object CassovaryServer extends TwitterServer {

  lazy override val log = Logger.get("CassovaryServer")

  def walkOn[V <: Node](graph: DirectedGraph[V]) {
    val numSteps = 100L * 100L
    val walkParams = RandomWalkParams(numSteps, 0.1, None, Some(2))
    val graphUtils = new GraphUtils(graph)
    log.info("Now doing a random walk of %s steps on a graph with %d nodes and %s edges...\n",
      numSteps, graph.nodeCount, graph.edgeCount)
    graphUtils.calculatePersonalizedReputation(0, walkParams)
    log.info("Done\n")
  }

  def main() {

    val service = new Service[Request, Response] {
      def apply(request: Request): Future[Response] = Future {
        val graph = TestGraphs.generateRandomGraph(100, 0.1)
        walkOn(graph)
        val content = "Finished walk on graph with %d nodes and %s edges\n".format(graph.nodeCount,
          graph.edgeCount)
        val response = request.response
        response.status = Status.Ok
        response.write(content)
        response
      }
    }

    // start Twitter Server
    val server = Httpx.serve(":8888", service)
    onExit {
      server.close()
    }
    Await.ready(server)
  }
}
