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
import java.io.{Writer, FileWriter}
import scala.collection.{Map => GeneralMap, mutable}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.matching.Regex
import scala.xml.pull.XMLEventReader
import scala.concurrent.duration.Duration

class RedirectsExtractor(inputFileName: String) extends WikipediaDumpProcessor {

  import ObfuscationTools._

  private val mainName = new mutable.HashMap[String, String]

  def getRedirects: collection.Map[String, String] = mainName

  val xml = new XMLEventReader(Source.fromFile(inputFileName))

  private def addNames(main: String, alternative: String) {
    mainName += ((alternative, main))
  }

  def onProcessingFinished() = ()

  def onPageEnd(pageName: String) = ()

  def processPageTextLine(pageName: String, text: String) {
    val redirectRegex = new Regex( """(?<=#REDIRECT \[\[)[^\]]+(?=\]\])""")
    redirectRegex.findAllIn(text).foreach {
      case redirectTo =>
        val obfuscatedPageName = obfuscate(pageName)
        val obfuscatedRedirectTo = obfuscate(redirectTo)
        if (validPageName(obfuscatedPageName) && validPageName(obfuscatedRedirectTo))
          addNames(obfuscatedRedirectTo, obfuscatedPageName)
    }
  }

  def onPageIdRead(pageName: String, pageId: Int) = ()
}

/**
 * Merges a sequence of maps String -> String into one String -> Set[String], such that
 * if `a: String -> b: String` is in any of input maps, there will be
 * `a: String -> C: Set[String]` in output map, such that `C` contains `b`.
 */
class RedirectsMerger {
  private val log = Logger.get("RedirectsMerger")

  def inverseMap[K, V](input: collection.Map[K, V]): Map[V, Set[K]] = {
    input.groupBy{ case (k, v) => v }.mapValues (_.map { case (k, v) => k }.toSet)
  }

  def mergeSetMaps[K, V](map1: Map[K, Set[V]], map2: Map[K, Set[V]]): Map[K, Set[V]] = {
    map1 ++ map2.map { case (k, v) => k -> (map1.getOrElse(k, Set()) ++ v)}
  }

  def mergeOtherNames(mainNames: Seq[collection.Map[String, String]]):
    collection.Map[String, Set[String]] = {
    def seqop(resultMap: Map[String, Set[String]],
              map: collection.Map[String, String]) = {
      mergeSetMaps(resultMap, inverseMap(map))
    }

    mainNames.par.aggregate(Map[String, Set[String]]())(seqop, mergeSetMaps)
  }

  def combineAndPrintRedirects(partialResutls: Seq[Future[collection.Map[String, String]]],
                               writer: Writer): Future[Unit] = {
    val done: Future[Unit] = for (allResults <- Future.sequence(partialResutls)) yield {
      log.info("Writing to file...")
      val otherNames = mergeOtherNames(allResults)
      otherNames.keysIterator foreach {
        mainName => writer.write(mainName + otherNames(mainName).mkString(splitChar,
          splitChar, "\n"))
      }
      writer.close()
    }
    done
  }
}

object RedirectsExtractor
  extends FilesProcessor[collection.Map[String, String]]("RedirectsExtractor")
  with App
{

  apply(args)

  def processFile(inputFilename: String): collection.Map[String, String] = {
    val extractor = new RedirectsExtractor(inputFilename)
    extractor()
    extractor.getRedirects
  }

  /**
   * Combines results from processing and writes them to a single file
   * specified by the `outputFlag`.
   */
  override def combineAndPrintResults(partialResutls: Seq[Future[collection.Map[String, String]]]): Unit = {
    val rm = new RedirectsMerger
    Await.ready(rm.combineAndPrintRedirects(partialResutls, writerFor(Some(outputFlag()))), Duration.Inf)
  }
}
