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

import scala.collection.mutable
import scala.io.Source
import scala.util.matching.Regex
import scala.xml.pull.XMLEventReader

/**
 * Converts wikipedia pages-articles dump file to adjacency list suitable for Cassovary.
 *
 * If `outputFilename` is None prints result to standard output.
 */
class DumpToGraphConverter(inputFilename: String, val outputFilename: Option[String])
  extends WikipediaDumpProcessor with Output {

  import ObfuscationTools._

  val minimumLinks = 2

  /**
   * Special prefixes of wikipedia links that point not to a normal page.
   */
  val specialLinkPrefixes = Array(
    "File:",
    "Image:",
    "User:",
    "user:",
    "d:", // discussion
    "Special:",
    "Extension:",
    "media:",
    "Special:", // special links
    "#", // link to anchor
    "{{", // talk page
    "//", // subpage (todo: can be treated other way)
    ":fr:", "fr:", // language codes
    ":es:", "es:",
    ":pt:", "pt:",
    ":de:", "de:",
    ":pl:", "pl:",
    ":it:", "it:",
    ":cn:", "cn:",
    ":en:", "en:",
    ":nl:", "nl:"
  )


  val xml = new XMLEventReader(Source.fromFile(inputFilename))

  var links = collection.mutable.LinkedHashSet[String]()

  val linksRegex = new Regex( """(?<=(\[\[))[^\]]+(?=(\]\]))""") //matches: [[ * ]]

  val printedPages = mutable.HashSet[String]()

  def writePage(pageName: String, links: collection.Set[String]) = {
    if (validPageName(pageName) && !printedPages.contains(pageName) && links.size >= minimumLinks) {
      write(pageName + "\t" + links.size)
      write(links.mkString("\n", "\n", "\n"))
      flush()
      printedPages += pageName
    }
  }

  override def processPageTextLine(pageName: String, text: String): Unit = {
    val linksInLine = linksRegex.findAllIn(text)
    linksInLine
      .filter(link => specialLinkPrefixes.find(prefix => link.startsWith(prefix)).isEmpty)
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

  override def onPageEnd(pageName: String): Unit = {
    writePage(obfuscate(pageName), links)
    links.clear()
  }

  override def onProcessingFinished(): Unit = {
    close()
  }

  override def onPageIdRead(pageName: String, pageId: Int): Unit = ()
}

object DumpToGraphConverter extends FilesProcessor[Unit]("DumpToGraphConverter") with App {

  def processFile(inputFilename: String, outputFilename: Option[String]) {
    new DumpToGraphConverter(inputFilename, outputFilename)()
  }

  apply(args)
}