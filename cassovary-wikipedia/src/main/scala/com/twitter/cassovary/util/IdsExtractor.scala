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
import java.io.{FileWriter, File, Writer}
import scala.Some
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.io.Source
import scala.xml.pull.XMLEventReader

class IdsExtractor(inputFileName: String, output: Writer) extends WikipediaDumpProcessor {
  val separator = " "

  override def xml: XMLEventReader = new XMLEventReader(Source.fromFile(inputFileName))

  override def onProcessingFinished(): Unit = ()

  override def onPageIdRead(pageName: String, pageId: Int): Unit = {
    // maybe should do this buffered
    output.write(pageName + separator + pageId + "\n")
  }

  override def onPageEnd(pageName: String): Unit = ()

  override def processPageTextLine(pageName: String, text: String): Unit = ()
}

object IdsExtractor extends App {
  private val log = Logger.get("IdsExtractor")

  val flags = new Flags("Wikipedia dump ids extractor")
  val fileFlag = flags[String]("f", "Filename of a single file to read from")
  val directoryFlag = flags[String]("d", "Directory to read all xml files from")
  val outputFlag = flags[String]("o", "Output filename to write to")
  val helpFlag = flags("h", false, "Print usage")
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
          log.warning("WARNING: empty directory.")
        }
        val output = new FileWriter(outputFlag())
        val futures: Array[Future[Unit]] = filesInDir
          .filter(f => f.endsWith("xml"))
          .map {
            file =>
              future {
                val extractor = new IdsExtractor(dirName + "/" + file, output) // todo: output lock
                extractor()
              }.recover {
                case t => log.warning("Exception thrown, when extracting file: " +
                  file + ": " + t.getLocalizedMessage)
              }
          }

        Await.ready(Future.sequence(futures.toList), Duration.Inf)
      case None =>
        val output = new FileWriter(outputFlag())
        val extractor = new IdsExtractor(fileFlag(), output)
        extractor()
    }
  }
}