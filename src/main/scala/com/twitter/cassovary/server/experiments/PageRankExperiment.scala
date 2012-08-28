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
package com.twitter.cassovary.server.experiments

import com.twitter.cassovary.server.{CachedDirectedGraphServerExperiment, CachedDirectedGraphServerConfig}
import com.twitter.cassovary.graph.CachedDirectedGraph
import com.twitter.cassovary.algorithms.{PageRankParams, PageRank}
import net.lag.logging.Logger
import com.twitter.cassovary.util.FileUtils

class PageRankExperiment(config: CachedDirectedGraphServerConfig,
                         graph:CachedDirectedGraph)
  extends CachedDirectedGraphServerExperiment(config, graph) {

  private val log = Logger.get(getClass.getName)

  def run {
    if (config.verbose) {
      log.info("Verbose PageRank = Write out each iteration")
      val params = new PageRankParams(iterations = Some(0))
      var pr = PageRank(graph, params)
      FileUtils.makeDirs(config.outputDirectory)
      (0 until config.iterations).foreach { i =>
        log.info("Beginning iteration %s".format(i))
        pr = PageRank.iterate(graph, new PageRankParams(iterations = Some(1)), pr)
        log.info("Writing to file...")
        FileUtils.writeDoubleArrayToFile(pr, "%s/%s_%s.txt".format(config.outputDirectory, System.nanoTime(), i))
      }
    }
    else {
      val i = config.iterations
      log.info("Regular PageRank = Write out only the final iteration")
      val params = new PageRankParams(iterations = Some(i))
      val pr = PageRank(graph, params)
      FileUtils.makeDirs(config.outputDirectory)
      log.info("Writing to file...")
      FileUtils.writeDoubleArrayToFile(pr, "%s/%s_%s.txt".format(config.outputDirectory, System.nanoTime(), i))
    }
  }

}
