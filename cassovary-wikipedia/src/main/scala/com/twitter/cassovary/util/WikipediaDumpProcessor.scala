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

import scala.xml.pull.{XMLEventReader, EvElemStart, EvElemEnd, EvText}

/**
 * [apply] method reads Wikipedia dumps in xml format.
 *
 * Note that the processor runs concurrently.
 */
trait WikipediaDumpProcessor {
  def xml: XMLEventReader

  def processPageTextLine(pageName: String, text: String): Unit

  def onPageEnd(pageName: String): Unit

  def onProcessingFinished(): Unit

  def onPageIdRead(pageName: String, pageId: Int): Unit

  def apply(): Unit = {
    var insidePage = false
    var insideTitle = false
    var insideText = false
    var insideId = false

    var currentPage: Option[String] = None
    for (event <- xml) {
      event match {
        case EvElemStart(_, "page", _, _) => {
          insidePage = true
          currentPage = Some("title not set")
        }
        case EvElemEnd(_, "page") => {
          onPageEnd(currentPage.get)
          currentPage = None
        }
        case EvElemStart(_, "title", _, _) if insidePage => {
          insideTitle = true
        }
        case EvElemStart(_, "id", _, _) if insidePage => {
          insideId = true
        }
        case EvElemStart(_, "text", _, _) if insidePage => {
          insideText = true
        }
        case EvElemEnd(_, "title") if insideTitle => {
          insideTitle = false
        }
        case EvElemEnd(_, "id") if insideTitle => {
          insideId = false
        }
        case EvElemEnd(_, "text") if insideText => {
          insideText = false
        }
        case EvText(t) =>
          if (insideTitle) {
            currentPage = Some(t)
          }
          if (insideId) {
            onPageIdRead(currentPage.get, t.toInt)
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
