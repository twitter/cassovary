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
package com.twitter.cassovary

import com.twitter.logging.Logger
import java.io.{FileOutputStream, FileInputStream, File}
import java.net.URL
import java.util.zip.GZIPInputStream
import scala.sys.process._

/**
 * Downloads and unpacks Gzip file specified by an URL.
 */
trait GzipGraphDownloader {

  private val log = Logger.get("GzGraphDownloader")

  private def downloadFile(url : String, targetPath : String) : Int = {
    (new URL(url) #> new File(targetPath)).!
  }

  private def unzipGzFile(inputFilename: String, outputFilename: String) {
    val BUFFER_SIZE = 1000

    val gzInput = new GZIPInputStream(new FileInputStream(new File(inputFilename)))
    val fileOutput = new FileOutputStream(outputFilename)
    val buffer = Array.ofDim[Byte](BUFFER_SIZE)
    var len = 0
    while ( {len = gzInput.read(buffer);len} > 0) {
      fileOutput.write(buffer, 0, len)
    }
    fileOutput.close()
    gzInput.close()
  }

  /**
   * Downloads a gzip file from a given {@code source} and unpacks it to single file {@code target}.
   */
  def downloadAndUnpack(source: String, target: String) {
    val tmpGzFile = target.split("\\.")(0) + ".gz"
    downloadFile(source, tmpGzFile)
    log.info("Unpacking... %s.", source)
    unzipGzFile(tmpGzFile, target)
    new File(tmpGzFile).delete()
  }
}
