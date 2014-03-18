package com.twitter.cassovary

import com.twitter.cassovary.algorithms.{PageRank, PageRankParams}
import com.twitter.cassovary.util.io.ListOfEdgesGraphReader
import com.twitter.cassovary.util.SequentialNodeRenumberer
import java.util.concurrent.Executors
import sys.process._
import java.net.URL
import java.io.{FileOutputStream, FileInputStream, File}
import com.twitter.util.Stopwatch
import java.util.zip.GZIPInputStream
import java.nio.file.{Paths, Files}
import com.twitter.cassovary.graph.GraphUtils
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams

/**
 * Performance test. Performs PageRank and Personalized PageRank algorithms
 * on several real life graphs from http://snap.stanford.edu/data/.
 *
 * Small examples are stored under cassovary-benchmarks/src/resources/graphs and include:
 *    * facebook (friends)
 *    * ca-GrQc (collaboration network)
 *    * p2pGnutella08 (p2p network)
 *    * wiki-vote (who-voted-on-whom network)
 *
 * Use 'large' parameter to perform the benchmark on large graphs downloaded
 * from web. They will be  stored in "cassovary-banchmarks/cache". The list
 * includes:
 *    * twitter (social circles)
 *    * cit-HepTh (collaboration network)
 *    * amazon0302 (similar products on amazon)
 */
object PageRankPerformanceBenchmark extends App {
  val CACHE_DIRECTORY = "cassovary-benchmarks/cache/"

  val SMALL_FILES_DIRECTORY = "cassovary-benchmarks/src/main/resources/graphs/"

  val (path, files) = if (args.length > 0 && args(0).equals("large")) {
    prepareLargeFiles()
    (CACHE_DIRECTORY, Seq("twitter", "citi-hepTh", "amazon0302"))
  } else {
    (SMALL_FILES_DIRECTORY, Seq("facebook", "ca-GrQc", "p2p-Gnutella08", "wiki-Vote"))
  }

  def measurePerformance(path : String, filename : String) {

    printf("Reading %s graph.\n", filename)
    val graph = new ListOfEdgesGraphReader(path, filename,
      new SequentialNodeRenumberer()) {
      override val executorService = Executors.newFixedThreadPool(2)
    }.toArrayBasedDirectedGraph()
    printf("Graph %s loaded form list of edges with %s nodes and %s edges.\n",
      filename, graph.nodeCount, graph.edgeCount)

    val timerPR = Stopwatch.start()
    PageRank(graph, PageRankParams())
    printf("\tPage rank (%s) (%s nodes, %s edges) time: %s.\n", filename, graph.nodeCount,
      graph.edgeCount, timerPR())

    val graphUtils = new GraphUtils(graph)
    val timerPPR = Stopwatch.start()
    graphUtils.calculatePersonalizedReputation(graph.maxNodeId, RandomWalkParams(20, 0.3))
    printf("\tPersonalized Page rank (%s) (%s nodes, %s edges) time: %s.\n", filename, graph.nodeCount,
      graph.edgeCount, timerPPR())

  }

  files.foreach{
    f => measurePerformance(path, f)
  }

  def downloadFile(url : String, targetPath : String) : Int = {
    (new URL(url) #> new File(targetPath)).!
  }

  def unzipGzFile(inputFilename: String, outputFilename: String) {
    val BUFFER_SIZE = 1000

    val gzInput = new GZIPInputStream(new FileInputStream(new File(inputFilename)))
    val fileOutput = new FileOutputStream(outputFilename)
    val buffer = Array.ofDim[Byte](BUFFER_SIZE)
    var len = 0
    while ( {len = gzInput.read(buffer);len} > 0) {
      fileOutput.write(buffer, 0, len)
    }
    fileOutput.close()
    gzInput.close()
  }

  def prepareLargeFiles() {
    new File(CACHE_DIRECTORY + "gz/").mkdirs()

    Map(
      "twitter" -> "http://snap.stanford.edu/data/twitter_combined.txt.gz",
      "citi-hepTh" -> "http://snap.stanford.edu/data/cit-HepTh.txt.gz",
      "amazon0302" -> "http://snap.stanford.edu/data/amazon0302.txt.gz"
    ).foreach{
      case (name, url) =>
        val path = Paths.get(CACHE_DIRECTORY + name + ".txt")
        if (!Files.exists(path)) {
          println("Downloading... " + name)
          downloadFile(url, CACHE_DIRECTORY + "gz/" + name + ".txt.gz")
          println("Unpacking... " + name)
          unzipGzFile(CACHE_DIRECTORY + "gz/" + name + ".txt.gz", CACHE_DIRECTORY + name + ".txt")
        }
    }
    println("Downloading files done.")
  }
}
