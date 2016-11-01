package com.twitter.cassovary.util.io

import java.io.{BufferedInputStream, FileInputStream}
import java.util.zip.GZIPInputStream

import com.twitter.cassovary.util.ParseString
import org.scalatest.{Matchers, WordSpec}

import scala.io.Source

class FileReaderSpec extends WordSpec with Matchers {

  private val directory: String = "cassovary-core/src/test/resources/graphs/"

  def allLines(fname: String, isGzip : Boolean = false): List[String] = {
    val source = if (isGzip)
      Source.fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(fname))), "ISO-8859-1")
    else
      Source.fromFile(fname)
    source.getLines().toList.filter(!_.startsWith("#"))
  }

  def checkStrings(fileName: String) {
    val pathName = directory + fileName
    val identityFileReader = new FileReader[String](pathName) {
      def processOneLine(l: String) = l
    }
    val expectedLines = allLines(pathName)
    val actualLines = identityFileReader.toList
    expectedLines shouldEqual actualLines
  }

  def checkPair[T](fileName: String, separator: Char = ' ', idReader: (String, Int, Int) => T, isGzip: Boolean = false) {
    val pathName = directory + fileName
    val actualLines = new TwoTsFileReader[Int](pathName, separator, ParseString.toInt, isGzip).toList
    val expectedLines = allLines(pathName, isGzip).map { l =>
      val arr = l.split(separator).map(_.toInt)
      (arr(0), arr(1))
    }
    expectedLines shouldEqual actualLines
  }

  def checkIntPair(fileName: String, separator: Char = ' '): Unit = {
    checkPair[Int](fileName, separator, ParseString.toInt)
  }

  def checkStringPair(fileName: String, separator: Char = ' '): Unit = {
    checkPair[String](fileName, separator, ParseString.identity)
  }

  def checkGzipIntPair(fileName: String, separator: Char = ' '): Unit = {
    checkPair[Int](fileName, separator, ParseString.toInt, true)
  }

  def checkAdjacency[T](fileName: String, separator: Char = ' ', idReader: (String, Int, Int) => T, isGzip: Boolean = false) {
    val pathName = directory + fileName
    val actualLines = new AdjacencyTsFileReader[Int](pathName, separator, ParseString.toInt, isGzip).toList
    val expectedLines = allLines(pathName, isGzip).map { l =>
      val arr = l.split(separator).map(_.toInt)
      if (arr.length > 1) {
        (arr(0), Some(arr(1)))
      } else {
        (arr(0), None)
      }
    }
    expectedLines shouldEqual actualLines
  }

  def checkIntAdjacency(fileName: String, separator: Char = ' '): Unit = {
    checkAdjacency[Int](fileName, separator, ParseString.toInt)
  }

  def checkGzipIntAdjacency(fileName: String, separator: Char = ' '): Unit = {
    checkAdjacency[Int](fileName, separator, ParseString.toInt, true)
  }

  "FileReader" when {
    "using strings" should {
      checkStrings("toy_3nodes.txt")
      checkStrings("toy_list5edges.txt")
      checkStrings("toy_6nodes_list_LongIds.txt")
    }

    "using pair of ids" should {
      checkIntPair("toy_list5edges.txt")
      checkIntPair("toy_6nodelabels_label1_int.txt")
      checkStringPair("toy_list5edges.txt")
    }

    "gzip test" should {
      checkGzipIntPair("gzip_toy_list5edges.txt.gz")
      checkGzipIntAdjacency("gzip_toy_6nodes_adj_1.txt.gz");
    }

    "adjacencyTsFileReader test" should {
      checkIntAdjacency("toy_6nodes_adj_1.txt")
      checkIntAdjacency("toy_6nodes_adj_2.txt")
    }
  }

}
