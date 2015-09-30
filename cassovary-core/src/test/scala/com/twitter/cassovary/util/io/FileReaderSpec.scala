package com.twitter.cassovary.util.io

import com.twitter.cassovary.util.ParseString
import org.scalatest.{Matchers, WordSpec}

import scala.io.Source

class FileReaderSpec extends WordSpec with Matchers {

  private val directory: String = "cassovary-core/src/test/resources/graphs/"

  def allLines(fname: String): List[String] = {
    Source.fromFile(fname).getLines().toList.filter(!_.startsWith("#"))
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

  def checkPair[T](fileName: String, separator: Char = ' ', idReader: (String, Int, Int) => T) {
    val pathName = directory + fileName
    val actualLines = new TwoTsFileReader[Int](pathName, separator, ParseString.toInt).toList
    val expectedLines = allLines(pathName).map { l =>
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

  "FileReader" when {
    "using strings" should {
      checkStrings("toy_3nodes.txt")
      checkStrings("toy_list5edges.txt")
      checkStrings("toy_6nodes_list_Longids.txt")
    }

    "using pair of ids" should {
      checkIntPair("toy_list5edges.txt")
      checkIntPair("toy_6nodelabels_label1_int.txt")
      checkStringPair("toy_list5edges.txt")
    }
  }

}
