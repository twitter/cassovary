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

import java.io.Writer
import scala.io.Source
import scala.xml.pull.XMLEventReader

class IdsExtractor(inputFilename: String, val writer: Writer)
  extends WikipediaDumpProcessor {

  def this(inputFilename: String, out: Option[String]) = this(inputFilename, writerFor(out))

  def xml = new XMLEventReader(Source.fromFile(inputFilename))

  def onProcessingFinished(): Unit = {
    writer.close()
  }

  def onPageIdRead(pageName: String, pageId: Int): Unit = {
    import ObfuscationTools._
    val obfuscated = obfuscate(pageName)
    if (validPageName(obfuscated)) {
      writer.write(obfuscated + splitChar + pageId + "\n")
    }
  }

  def onPageEnd(pageName: String): Unit = {}

  def processPageTextLine(pageName: String, text: String): Unit = ()
}

object IdsExtractor extends App {

  val fileProcessor = new FilesProcessor[Unit]("IdsExtractor") {
    override def processFile(inputFilename: String): Unit = {
      val extractor = new IdsExtractor(inputFilename, Some(inputFilename.replace(".xml",
        extensionFlag())))
      extractor()
    }
  }

  fileProcessor.apply(args)
}
