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
import java.io.FileWriter
import scala.collection.{Map => GeneralMap, mutable}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source
import scala.util.matching.Regex
import scala.xml.pull.XMLEventReader

class RedirectsExtractor(inputFileName: String) extends WikipediaDumpProcessor {

  import ObfuscationTools._

  private val mainName = new mutable.HashMap[String, String]

  def getRedirects = mainName

  val xml: XMLEventReader = new XMLEventReader(Source.fromFile(inputFileName))

  private def addNames(main: String, alternative: String) {
    mainName += ((main, alternative))
  }

  override def onProcessingFinished(): Unit = ()

  override def onPageEnd(pageName: String): Unit = ()

  override def processPageTextLine(pageName: String, text: String): Unit = {
    val redirectRegex = new Regex( """(?<=#REDIRECT \[\[)[^\]]+(?=\]\])""")
    redirectRegex.findAllIn(text).foreach {
      case redirectTo =>
        val obfuscatedPageName = obfuscate(pageName)
        val obfuscatedRedirectTo = obfuscate(redirectTo)
        if (validPageName(obfuscatedPageName) && validPageName(obfuscatedRedirectTo))
          addNames(obfuscatedRedirectTo, obfuscatedPageName)
    }
  }

  override def onPageIdRead(pageName: String, pageId: Int): Unit = ()
}

object RedirectsExtractor
  extends FilesProcessor[mutable.HashMap[String, String]]("RedirectsExtractor")
  with App
{

  private val log = Logger.get("RedirectsExtractor")

  val splitChar = " "

  apply(args)

  override def processFile(inputFilename: String,
                           outputFilename: Option[String]): mutable.HashMap[String, String] = {
    val extractor = new RedirectsExtractor(inputFilename)
    extractor()
    extractor.getRedirects
  }

  /**
   * Combines results from processing and writes them to a single file
   * specified by the `outputFlag`.
   */
  override def combineResults(partialResutls: Seq[Future[mutable.HashMap[String, String]]]): Unit = {
    for (allResults <- Future.sequence(partialResutls)) {
      write(allResults)
    }
  }

  def mergeOtherNames(mainNames: Seq[GeneralMap[String, String]]):
  GeneralMap[String, collection.Set[String]] = {
    def seqop(resultMap: mutable.Map[String, mutable.Set[String]],
              map: GeneralMap[String, String]) = {
      map.foreach {
        case (k, v) => resultMap.getOrElseUpdate(k, mutable.Set.empty[String]) += v
      }
      resultMap
    }

    def comboop(map1: mutable.Map[String, mutable.Set[String]],
                map2: mutable.Map[String, mutable.Set[String]]) = {
      map2.foreach {
        case (k, v) =>
          map1.getOrElseUpdate(k, mutable.Set.empty[String]) ++= v
      }
      map1
    }
    mainNames.par.aggregate(mutable.Map[String, mutable.Set[String]]())(seqop, comboop)
  }

  def write(otherNamesMaps: Seq[collection.Map[String, String]]) {
    log.info("Writing to file...")
    val writer = new FileWriter(outputFlag())
    val otherNames = mergeOtherNames(otherNamesMaps)
    otherNames.keysIterator foreach {
      mainName => writer.write(mainName + otherNames(mainName).mkString(splitChar,
        splitChar, "\n"))
    }
    writer.close()
  }
}
