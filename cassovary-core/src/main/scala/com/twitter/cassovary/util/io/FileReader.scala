package com.twitter.cassovary.util.io

import java.io.IOException

import com.twitter.logging.Logger
import com.twitter.util.NonFatal
import java.io.FileInputStream
import java.io.BufferedInputStream
import java.util.zip.GZIPInputStream
import scala.io.Source


abstract class FileReader[T](fileName: String, isGzip: Boolean = false) extends Iterator[T] {
  protected val log = Logger.get()
  protected var lastLineParsed = 0
  log.info("Starting reading from file %s...\n", fileName)
  private val source = if (isGzip)
    Source.fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(fileName))), "ISO-8859-1")
  else
    Source.fromFile(fileName)

  private val lines = source.getLines().map { x =>
    lastLineParsed += 1
    x
  }

  private var _next: Option[T] = checkNext()

  def hasNext: Boolean = _next.isDefined
  def next(): T = {
    val curr = saveCurr(_next)
    _next = checkNext()
    if (!hasNext) {
      log.info("Finished reading from file %s...\n", fileName)
      close()
    }
    curr.get
  }

  protected def processOneLine(line: String): T

  // by default, to save state, we simply return x
  // but this is available to be overridden should x
  // be a reference that might be lost.
  protected def saveCurr(x: Option[T]) = x

  private def checkNext(): Option[T] = {
    var found: Option[T] = None
    while (lines.hasNext && found.isEmpty) {
      val line = lines.next().trim
      if (line.charAt(0) != '#') {
        try {
          found = Some(processOneLine(line))
        } catch {
          case NonFatal(exc) =>
            throw new IOException("Parsing failed near line: %d in %s"
                .format(lastLineParsed, fileName), exc)
        }
      }
    }
    found
  }

  def close(): Unit = {
    source.close()
  }

}

// T is the id type, typically Int or Long or String
class TwoTsFileReader[T](fileName: String,
    separator: Char,
    idReader: (String, Int, Int) => T, isGzip: Boolean = false)
    extends FileReader[(T, T)](fileName, isGzip) {

  def processOneLine(line: String): (T, T) = {
    val i = line.indexOf(separator)
    if (i == -1) {
      log.error(s"!!Parse error at line number $lastLineParsed in" +
        s"file $fileName. Parsing using separator: ${separator.toInt} (in Int) and '$separator' (in Char)!!\n")
      throw new IOException("Parse error")
    }
    val source = idReader(line, 0, i - 1)
    val dest = idReader(line, i + 1, line.length - 1)
    (source, dest)
  }

}

// Here, T is either the source or dest id (typically Int or Long or String)
//   and "Int" represents the count of adjacency. 
class AdjacencyTsFileReader[T](fileName: String,
                               separator: Char,
                               idReader: (String => T), isGzip: Boolean = false)
  extends FileReader[(T, Option[Int])](fileName, isGzip) {

  def processOneLine(line: String): (T, Option[Int]) = {
    val i = line.indexOf(separator)
    if (i == -1) {
      val dest = idReader(line)
      (dest, None)
    } else {
      val source = idReader(line.substring(0, i))
      val count = line.substring(i + 1, line.length).toInt
      (source, Some(count))
    }
  }

}