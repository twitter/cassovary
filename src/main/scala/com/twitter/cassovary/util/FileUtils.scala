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
package com.twitter.cassovary.util

import scala.io.Source
import java.io.{PrintWriter, File}

/**
 * Miscellaneous convenience functions for working with files
 */
object FileUtils {

  /**
   * Return a Java PrintWriter to be used in a function
   * @param f File to act on
   * @param op Function that accepts a PrintWriter
   */
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  /**
   * Return a Java PrintWriter but don't close
   * @param filename Filename to act on
   * @return a PrintWriter
   */
  def printWriter(filename: String) = {
    new PrintWriter(new File(filename))
  }

  /**
   * Read lines in from a file
   * @param filename Filename to read from
   * @param op Function that accepts single lines
   */
  def linesFromFile(filename: String)(op: String => Unit) {
    Source.fromFile(filename).getLines().foreach(op)
  }

  /**
   * Make directories recursively if they don't exist
   * @param directory
   */
  def makeDirs(directory: String) {
    new File(directory).mkdirs()
  }

  /**
   * Write out an array of doubles to a file
   * @param a Double array
   * @param filename filename to write to
   */
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

  /**
   * Read in lines from a file and provide a PrintWriter to another
   * @param inFilename file to read in lines from
   * @param outFilename file to which PrintWriter is attached
   * @param op function that accepts a read-in line and the PrintWriter object
   */
  def readLinesAndPrintToFile(inFilename: String, outFilename: String)
                             (op: (String, java.io.PrintWriter) => Unit) {
    printToFile(new File(outFilename)) { p =>
      linesFromFile(inFilename) { line =>
        op(line, p)
      }
    }
  }
}
