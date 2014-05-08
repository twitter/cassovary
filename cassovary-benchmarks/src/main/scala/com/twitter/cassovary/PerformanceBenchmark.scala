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
package com.twitter.cassovary

import com.twitter.cassovary.graph._
import com.twitter.cassovary.util.io.ListOfEdgesGraphReader
import com.twitter.cassovary.util.SequentialNodeRenumberer
import com.twitter.app.Flags
import com.twitter.util.Stopwatch
import java.util.concurrent.Executors
import java.io.File
import scala.collection.mutable.ListBuffer

/**
 * Performance test.
 *
 * Performs PageRank and Personalized PageRank algorithms
 * on several real life graphs from http://snap.stanford.edu/data/. Two small
 * graphs are stored under resources, you can benchmark on larger graphs
 * by providing additional graph urls (they will be downloaded).
 *
 * Usage:
 *   PerformanceBenchmark [-s] [-g] [-p] [url ...]
 *    -g  : Benchmarks PageRank algorithm (global)
 *    -p  : Benchmarks Personalized PageRank algorithm
 *    -s  : Benchmarks small graphs stored under src/resources:
 *         * ego-facebook (friends)
 *         * wiki-vote (who-voted-on-whom network)
 *    url : Downloads gzipped graph in list of edges format and
 *          performs benchmarks on it.
 *
 *          url points to a gzipped graph in a list of edges format.
 *
 * If neither -s nor urls are provided, performs benchmark on
 * small graphs.
 *
 * If neither -g nor -p are provided, performs benchmark on both: PageRank
 * and PersonalizedPageRank.
 *
 * Examples:
 *  PerformanceBenchmark -g -s http://snap.stanford.edu/data/cit-HepTh.txt.gz
 *
 *    Benchmarks PageRank on small graphs and on cit-HepTh graph from SNAP.
 *
 *  PerformanceBenchmark http://snap.stanford.edu/data/cit-HepTh.txt.gz
 *                       http://snap.stanford.edu/data/p2p-Gnutella08.txt.gz
 *                       http://snap.stanford.edu/data/amazon0302.txt.gz
 *                       http://snap.stanford.edu/data/soc-Epinions1.txt.gz
 *
 *    Performs benchmark on cit-HepTh, p2p-Gnutella08, amazon0302, soc-Epinoins1 graphs
 *    for both PageRank and Personalized Page Rank.
 *
 * By default runs every test 3 times and reports average time taken.
 *
 * See: {@link http://snap.stanford.edu/data/}
 */

object PerformanceBenchmark extends App with GzipGraphDownloader {
  /**
   * Directory to store cached graphs downloaded from the web.
   */
  val CACHE_DIRECTORY = "cassovary-benchmarks/cache/"

  /**
   * Path to the directory storing small graphs.
   */
  val SMALL_FILES_DIRECTORY = "cassovary-benchmarks/src/main/resources/graphs/"

  /**
   * Files to be benchmarked as a list of (directory, name) pairs.
   */
  val files = ListBuffer[(String, String)]()

  lazy val smallFiles = List((SMALL_FILES_DIRECTORY, "facebook"), (SMALL_FILES_DIRECTORY, "wiki-Vote"))

  /**
   * Builders of algorithms to be benchmarked.
   */
  val benchmarks = ListBuffer[(DirectedGraph => OperationBenchmark)]()

  /**
   * Thread pool used for reading graphs. Only useful if multiple files with the same prefix name are present.
   */
  val graphReadingThreadPool = Executors.newFixedThreadPool(4)

  /**
   * Number of repeats of every benchmark.
   */
  val DEFAULT_REPS = 10
  val defaultLocalGraphFile = "facebook"

  val flags = new Flags("Performance benchmark")
  val localFileFlag = flags("local", defaultLocalGraphFile,
    "Specify common prefix of local files in " + SMALL_FILES_DIRECTORY)
  val remoteFileFlag = flags("url",
    "http://snap.stanford.edu/data/cit-HepTh.txt.gz",
    "Specify a URL to download a graph file from")
  val helpFlag = flags("h", false, "Print usage")
  val globalPRFlag = flags("globalpr", false, "run global pagerank benchmark")
  val pprFlag = flags("ppr", false, "run personalized pagerank benchmark")
  val reps = flags("reps", DEFAULT_REPS, "number of times to run benchmark")
  flags.parse(args)
  if (localFileFlag.isDefined) files += ((SMALL_FILES_DIRECTORY, localFileFlag()))
  if (remoteFileFlag.isDefined) files += cacheRemoteFile(remoteFileFlag())
  if (files.isEmpty) {
    println("No files specified on command line. Taking default graph files facebook and wiki-Vote.")
    files ++= smallFiles
  }
  if (globalPRFlag()) { benchmarks += (g => new PageRankBenchmark(g)) }
  if (pprFlag()) { benchmarks += (g => new PersonalizedPageRankBenchmark(g)) }
  if (helpFlag()) { println(flags.usage)}

  if (benchmarks.isEmpty) {
    println("No benchmarks specified on command line. Will only read the graph files.")
  }

  files.foreach {
    case (path, filename) =>
      printf("Reading %s graph.\n", filename)
      val readingTime = Stopwatch.start()
      val graph = readGraph(path, filename)
      printf("\tGraph %s loaded from list of edges with %s nodes and %s edges.\n" +
             "\tLoading Time: %s\n", filename, graph.nodeCount, graph.edgeCount, readingTime())
      for (b <- benchmarks) {
        val benchmark = b(graph)
        printf("Running benchmark %s on graph %s...\n", benchmark.name, filename)
        val duration = benchmark.run(reps())
        printf("\tAvg time over %d repetitions: %s.\n", reps(), duration)
      }
  }

  graphReadingThreadPool.shutdown()

  def readGraph(path : String, filename : String) : DirectedGraph = {
    new ListOfEdgesGraphReader(path, filename,
      new SequentialNodeRenumberer()) {
      override val executorService = graphReadingThreadPool
    }.toArrayBasedDirectedGraph()
  }

  def cacheRemoteFile(url : String) : (String, String) = {
    printf("Downloading remote file from %s\n", url)
    new File(CACHE_DIRECTORY).mkdirs()
    val name = url.split("/").last.split("\\.")(0) + ".txt"
    val target =  CACHE_DIRECTORY + name
    downloadAndUnpack(url, target)
    (CACHE_DIRECTORY, name)
  }
}
