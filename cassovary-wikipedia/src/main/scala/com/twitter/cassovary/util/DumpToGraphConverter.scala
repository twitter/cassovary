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
import scala.collection.mutable
import scala.io.Source
import scala.util.matching.Regex
import scala.xml.pull.XMLEventReader

/**
 * Converts wikipedia pages-articles dump file to adjacency list suitable for Cassovary.
 *
 * If `outputFilename` is None prints result to standard output.
 */
class DumpToGraphConverter(inputFilename: String, writer: Writer)
  extends WikipediaDumpProcessor {

  import ObfuscationTools._

  def this(inputFilename: String, out: Option[String]) = this(inputFilename, writerFor(out))

  val minimumLinks = 2

  val xml = new XMLEventReader(Source.fromFile(inputFilename))

  var links = collection.mutable.LinkedHashSet[String]()

  val linksRegex = new Regex( """(?<=(\[\[))[^\]]+(?=(\]\]))""") //matches: [[ * ]]

  val printedPages = mutable.HashSet[String]()

  def writePage(pageName: String, links: collection.Set[String]) {
    if (validPageName(pageName) && !printedPages.contains(pageName) && links.size >= minimumLinks) {
      writer.write(pageName + "\t" + links.size)
      writer.write(links.mkString("\n", "\n", "\n"))
      printedPages += pageName
    }
  }

  def processPageTextLine(pageName: String, text: String) {
    val linksInLine = linksRegex.findAllIn(text)
    linksInLine
      .foreach {
      case link: String =>
        link.split("\\|") match {
          case Array(head, _*) =>
            val obfuscated = obfuscate(head)
            if (validPageName(obfuscated))
              links += obfuscated
          case _ =>
            // link empty
        }
      case _ => ()
    }
  }

  def onPageEnd(pageName: String) {
    writePage(obfuscate(pageName), links)
    links.clear()
  }

  def onProcessingFinished() {
    writer.close()
  }

  def onPageIdRead(pageName: String, pageId: Int) = ()
}

object DumpToGraphConverter extends App {

  val fileProcessor = new FilesProcessor[Unit]("DumpToGraphConverter") {
    def processFile(inputFilename: String) {
      new DumpToGraphConverter(inputFilename, Some(inputFilename.replace(".xml", extensionFlag())))()
    }
  }

  fileProcessor.apply(args)
}
