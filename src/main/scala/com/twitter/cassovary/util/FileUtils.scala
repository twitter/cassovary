package com.twitter.cassovary.util

import io.Source
import java.io.{PrintWriter, File}

object FileUtils {
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def printWriter(filename: String) = {
    new PrintWriter(new File(filename))
  }

  def linesFromFile(filename: String)(op: String => Unit) {
    Source.fromFile(filename).getLines().foreach(op)
  }

  def makeDirs(directory: String) {
    new File(directory).mkdirs()
  }

  def doubleArrayToFile(a:Array[Double], filename: String) {
    printToFile(new File(filename)) { p =>
      var i = 0
      while (i < a.size) {
        if (a(i) != 0) {
          p.println("%s %s".format(i, a(i)))
        }
        i += 1
      }
    }
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
