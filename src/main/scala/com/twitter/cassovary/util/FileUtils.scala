package com.twitter.cassovary.util

import io.Source
import java.io.File

object FileUtils {
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def linesFromFile(filename: String)(op: String => Unit) {
    Source.fromFile(filename).getLines().foreach(op)
  }

  def readLinesAndPrintToFile(inFilename: String, outFilename: String)
                             (op: (String, java.io.PrintWriter) => Unit) {
    printToFile(new File(outFilename)) { p =>
      linesFromFile(inFilename) { line =>
        op(line, p)
      }
    }
  }
}
