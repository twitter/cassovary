package com.twitter.cassovary.util.io

import com.twitter.app.Flags
import java.io.{FileWriter, File}
import java.util.concurrent.{Executors, ExecutorService}

class ListOfEdgesToAdjacencyListConverter(
  inputDirectory: String,
  inputFileNamesPrefix: String,
  outputDirectory: String,
  outputFileNamesPrefix: String,
  outputFileNamesExtension: String,
  numberOfOutputChunks: Int,
  threadPool: ExecutorService
) {
  def apply() {
    def outputFileName(chunkNumber: Int) = outputFileNamesPrefix + "_" + chunkNumber + "." + outputFileNamesExtension
    val graph = ListOfEdgesGraphReader.forIntIds(inputDirectory, inputFileNamesPrefix, threadPool)
      .toArrayBasedDirectedGraph()
    val outputWriters = Seq.tabulate(numberOfOutputChunks)(n => new File(outputDirectory, outputFileName(n)))
      .map(file => new FileWriter(file))
    GraphWriter.writeDirectedGraph(graph, outputWriters)
  }
}

object ListOfEdgesToAdjacencyListConverter extends App {

  val flags = new Flags("List of edges to adjacency list graph converter")
  val inputDirectoryFlag = flags[String]("d", "Input directory to read from")
  val inputFilenamePrefixFlag = flags[String]("f", "Prefix of filenames to read from")
  val outputDirectoryFlag = flags[String]("od", "", "Output direcotry to write to (default: equal to input directory)")
  val outputPrefixFlag = flags[String]("of", "Output files prefix")
  val chunksFlag = flags[Int]("n", 32, "Number of chunks to write to")
  val extensionFlag = flags[String]("e", ".graph", "Extension of files to write to.")
  val helpFlag = flags("h", false, "Print usage")
  flags.parse(args)

  if (helpFlag()) {
    println(flags.usage)
  } else {
    val threadPool = Executors.newFixedThreadPool(4)
    val outputDirectory = if (outputDirectoryFlag().isEmpty) inputDirectoryFlag() else outputDirectoryFlag()
    new ListOfEdgesToAdjacencyListConverter(inputDirectoryFlag(), inputFilenamePrefixFlag(),
      outputDirectory, outputPrefixFlag(), extensionFlag(), chunksFlag(), threadPool)()
    threadPool.shutdown()
  }
}