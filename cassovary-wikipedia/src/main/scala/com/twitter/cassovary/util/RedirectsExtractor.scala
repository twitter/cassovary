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
import java.io.{File, FileWriter}
import scala.Some
import scala.collection.{Map => GeneralMap}
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, future}
import scala.io.Source
import scala.util.matching.Regex
import scala.xml.pull.XMLEventReader

class RedirectsExtractor(inputFileName: String) extends WikipediaDumpProcessor {

  import ObfuscationTools._

  val mainName = new mutable.HashMap[String, String]

  val xml: XMLEventReader = new XMLEventReader(Source.fromFile(inputFileName))

  private def addNames(main: String, alternative: String) {
    mainName += ((main, alternative))
  }

  override def onProcessingFinished(): Unit = ()

  override def onPageEnd(pageName: String): Unit = ()

  override def processPageTextLine(pageName: String, text: String): Unit = {
    val redirectRegex = new Regex("""(?<=#REDIRECT \[\[)[^\]]+(?=\]\])""")
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

object RedirectsExtractor extends App {

  private val log = Logger.get("RedirectsExtractor")

  val flags = new Flags("Wikipedia dump multiple page title resolver")
  val fileFlag = flags[String]("f", "Filename of a single file to read from")
  val directoryFlag = flags[String]("d", "Directory to read all xml files from")
  val outputFlag = flags[String]("o", "Output filename to write to")
  val helpFlag = flags("h", false, "Print usage")
  flags.parse(args)

  val splitChar = "|"

  def mergeOtherNames(mainNames: List[GeneralMap[String, String]]):
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
    mainNames.par.aggregate(mutable.Map[String, mutable.Set[String]]()) (seqop, comboop)
  }

  def write(otherNamesMaps : List[collection.Map[String, String]]) {
    log.info("Writing to file...")
    val writer = new FileWriter(outputFlag())
    val otherNames = mergeOtherNames(otherNamesMaps)
    otherNames.keysIterator foreach {
      mainName => writer.write(mainName + otherNames(mainName).mkString(splitChar, splitChar, "\n"))
    }
    writer.close()
  }

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
        val futures: Array[Future[GeneralMap[String, String]]] = filesInDir
          .filter(file => file.endsWith("xml"))
          .map {
            file =>
              future {
                val extractor = new RedirectsExtractor(dirName + "/" + file)
                extractor()
                extractor.mainName
              }.recover {
                case t => log.warning("Exception thrown, when extracting file: " +
                  file + ": " + t.getLocalizedMessage)
                Map.empty[String, String]
              }
          }
        val finished = Future.sequence(futures.toList).map {
          case a =>
            write(a)
        }

        Await.ready(finished, Duration.Inf)
      case None =>
        val extractor = new RedirectsExtractor(fileFlag())
        extractor()
        write(List(extractor.mainName))
    }
  }
}
