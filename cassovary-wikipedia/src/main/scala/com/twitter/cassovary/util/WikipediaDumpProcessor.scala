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

import com.twitter.logging.Logger
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.xml.pull.{XMLEventReader, EvElemStart, EvElemEnd, EvText}

trait WikipediaDumpProcessor {
  private val log = Logger.get("WikipediaDumpProcessor")

  def xml: XMLEventReader

  def processPageTextLine(pageName: String, text: String): Unit

  def onPageEnd(pageName: String): Unit

  def onProcessingFinished(): Unit

  def onPageIdRead(pageName: String, pageId: Int): Unit

  def apply(): Unit = {
    var insidePage = false
    var insideTitle = false
    var insideText = false
    var insidePageId = false

    val path = mutable.Stack[String]()

    var currentPage: Option[String] = None
    for (event <- xml) {
      event match {
        case EvElemStart(_, "page", _, _) => {
          insidePage = true
          currentPage = Some("title not set")
          path.push("page")
        }
        case EvElemEnd(_, "page") => {
          onPageEnd(currentPage.get)
          currentPage = None
          path.pop()
        }
        case EvElemStart(_, "title", _, _) if insidePage => {
          insideTitle = true
          path.push("title")
        }
        case EvElemStart(_, "id", _, _) if insidePage => {
          if (path.top == "page")
            insidePageId = true
          path.push("id")
        }
        case EvElemStart(_, "text", _, _) if insidePage => {
          path.push("text")
          insideText = true
        }
        case EvElemEnd(_, "title") => {
          insideTitle = false
          path.pop()
        }
        case EvElemEnd(_, "id") => {
          insidePageId = false
          path.pop()
        }
        case EvElemEnd(_, "text") => {
          insideText = false
          path.pop()
        }
        case EvElemStart(_, n, _, _) => {
          path.push(n)
        }
        case EvElemEnd(_, n) => {
          path.pop()
        }
        case EvText(t) if !t.trim.isEmpty =>
          if (insideTitle) {
            currentPage = Some(t)
          }
          if (insidePageId) {
            try {
              onPageIdRead(currentPage.get, t.trim.toInt)
            } catch {
              case e: NumberFormatException =>
                log.warning("Unable to read page id (%s).".format(currentPage.get))
            }
          }
          if (insideText) {
            processPageTextLine(currentPage.get, t)
          }
        case _ => // ignore
      }
    }
    onProcessingFinished()
  }
}

object WikipediaDumpsProcessor {
  private val log = Logger.get("WikipediaDumpsProcessor")

  def main(args: Array[String]) {
    val fileProcessor = new FilesProcessor[collection.Map[String, String]]("WikipediaDumpProcessor") {

      override def processFile(inputFilename: String):
      collection.Map[String, String] = {
        new IdsExtractor(inputFilename, Some(inputFilename.replace(".xml",
          extensionFlag() + "ids")))()
        log.info("Ids extracted")

        new DumpToGraphConverter(inputFilename, Some(inputFilename.replace(".xml",
          extensionFlag() + "graph")))()
        log.info("Links to graph format converted")

        val extractor = new RedirectsExtractor(inputFilename)
        extractor()
        extractor.getRedirects
      }

      override def combineAndPrintResults(partialResults: Seq[Future[collection.Map[String, String]]]): Unit = {
        val rm = new RedirectsMerger
        val done = rm.combineAndPrintRedirects(partialResults, writerFor(Some(directoryFlag() + "/" + "Wikipedia.red")))
        Await.ready(done, Duration.Inf)
        log.info("Redirects extracted and merged")
      }
    }

    fileProcessor.apply(args)
  }
}
