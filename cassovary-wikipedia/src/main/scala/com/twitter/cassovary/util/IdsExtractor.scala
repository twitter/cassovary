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

import scala.io.Source
import scala.xml.pull.XMLEventReader

class IdsExtractor(inputFileName: String, val outputFilename: Option[String] = None)
  extends WikipediaDumpProcessor with Output {
  val separator = " "

  override def xml: XMLEventReader = new XMLEventReader(Source.fromFile(inputFileName))

  override def onProcessingFinished(): Unit = {
    close()
  }

  override def onPageIdRead(pageName: String, pageId: Int): Unit = {
    import ObfuscationTools._
    val obfuscated = obfuscate(pageName)
    if (validPageName(obfuscated)) {
      write(obfuscated + separator + pageId + "\n")
    }
  }

  override def onPageEnd(pageName: String): Unit = {}

  override def processPageTextLine(pageName: String, text: String): Unit = ()
}

object IdsExtractor extends FilesProcessor[Unit]("IdsExtractor") with App {

  override def processFile(inputFilename: String, outputFilename: Option[String]): Unit = {
    val extractor = new IdsExtractor(inputFilename, outputFilename)
    extractor()
  }

  apply(args)
}