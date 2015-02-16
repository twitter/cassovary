/*
 * Copyright 2015 Twitter, Inc.
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
package com.twitter.cassovary.util.io

import com.twitter.cassovary.graph.labels._
import com.twitter.util.NonFatal
import java.io.IOException
import scala.io.Source

/**
 * Only reads node labels of type int right now and only uses array based label.
 * Assumes that the label files are named as follows:
 * `collPrefix`_anything_labelName.txt
 * So the file name starts with an identifier that marks this collection of labels to be read.
 * And that each line has an id followed by a single space followed by int value of label
 * <id> <labelValue>
 */
class LabelsReader(val directory: String, val collPrefix: String) {

  private class IntLabelReaderMaxId(fileName: String, maxId: Int) {
    def readOneLabel(): Label[Int, Int] = {
      val fileNameParts = fileName.split(Array('_', '.'))
      val name = fileNameParts(fileNameParts.length - 2)
      val label = new ArrayBasedLabel[Int](name, maxId)
      var lastLineParsed = 0
      val separator = " "
      val labelPattern = ("""^(\w+)""" + separator + """(\d+)""").r
      val source = Source.fromFile(fileName)
      source.getLines().foreach { line =>
        lastLineParsed += 1
        try {
          val labelPattern(id, labelValue) = line
          label.update(id.toInt, labelValue.toInt)
        } catch {
          case NonFatal(exc) =>
            throw new IOException("Parsing failed near line: %d in %s"
              .format(lastLineParsed, fileName), exc)
        }
      }
      source.close()
      label
    }
  }

  def read(maxId: Int): Labels[Int] = {
    val labelFiles = IoUtils.readFileNames(directory, collPrefix)
    val labels = new Labels[Int]
    labelFiles foreach { fileName =>
      labels += new IntLabelReaderMaxId(fileName, maxId).readOneLabel()
    }
    labels
  }

}