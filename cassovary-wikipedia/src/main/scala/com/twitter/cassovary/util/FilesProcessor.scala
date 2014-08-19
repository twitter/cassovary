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

package com.twitter.cassovary.util

import com.twitter.app.Flags
import com.twitter.logging.Logger
import java.io.File
import scala.Some
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration

/**
 * General schema for processing multiple files (in our case wikipedia dumps)
 * and combining results from their processing.
 *
 * Reads information from a list of files, processes every file with
 * [processFile] function (implemented in subclass) and combines
 * results from all files using [combineResults] (default implementation
 * waits for first phase finish only).
 *
 * @param name Program name to be used by `flags`.
 * @tparam R Result of processing single file.
 */
abstract class FilesProcessor[R](name: String) {
  private val log = Logger.get("Files processor")

  /**
   * @return Default extension of output file.
   */
  protected def defaultExtension = ".wiki"

  val flags = new Flags(name)
  val fileFlag = flags[String]("file", "Filename of a single file to read from")
  val directoryFlag = flags[String]("dir", "Directory to read all xml files from")
  val outputFlag = flags[String]("out", "Output filename to write to")
  val extensionFlag = flags[String]("extension", defaultExtension, "Extension of file to write to " +
    "(when processing multiple files.)")
  val helpFlag = flags("h", false, "Print usage")

  def apply(args: Array[String]): Unit = {
    flags.parse(args)

    if (helpFlag()) {
      println(flags.usage)
    } else {
      directoryFlag.get match {
        case Some(dirName) =>
          val dir = new File(dirName)
          val filesInDir = dir.list()
          if (filesInDir == null) {
            throw new Exception("Current directory is " + System.getProperty("user.dir") +
              " and nothing was found in dir " + dir)
          }
          if (filesInDir.isEmpty) {
            log.warning("Empty directory.")
          }
          val futures: Array[Future[R]] = filesInDir
            .filter(f => f.endsWith("xml"))
            .map {
            file =>
              future {
                try {
                  processFile(dir + "/" + file)
                } catch {
                  case e: Exception => throw new Exception("Exception thrown, when " +
                    "extracting file: " + file, e)
                }
              }
          }
          combineAndPrintResults(futures)
        case None =>
          val res = processFile(fileFlag())
          combineAndPrintResults(Seq(Future.successful(res)))
      }
    }
  }

  def processFile(inputFilename: String): R

  /**
   * Combines results from file processing phase.
   *
   * Default implementation only waits for all the files to be
   * processed. For each failed processing puts warning to the log
   * and ignores the failure.
   */
  def combineAndPrintResults(partialResults: Seq[Future[R]]): Unit = {
    partialResults.foreach {
      f: Future[R] => f.onFailure {
        case t: Throwable => log.warning(t.getMessage)
      }
    }
    Await.ready(Future.sequence(partialResults), Duration.Inf)
  }
}
