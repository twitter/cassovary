/*
 * Copyright 2012 Twitter, Inc.
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
import com.twitter.cassovary.util.{FileUtils, LoaderSerializerReader, Renumberer}
import java.io.File

object RenumbererMapper {
  def main(args: Array[String]) {

    var renumberer: Renumberer = null
    if (args.size > 0) {
      try {
        renumberer = new Renumberer(args(0).toInt)
      }
      catch {
        case e: Exception =>
          renumberer = new Renumberer(1)
          renumberer.fromReader(new LoaderSerializerReader(args(0)))
      }
    }

    // RenumbererMapper path/to/mapping path/to/inputIds path/to/outputIndices
    if (args.size > 2) {
      println("Reading ids from %s and writing forward mappings to %s...".format(args(1), args(2)))
      FileUtils.readLinesAndPrintToFile(args(1), args(2)) { (l, p) =>
        p.println(renumberer.translate(l.toInt))
      }
    }

    println("f = forward map, r = reverse map")

    while (true) {
      print("Type in [f/r][number]: ")
      val in = readLine()

      try {
        val num = in.substring(1).stripLineEnd.toInt
        val dir = in.substring(0, 1)

        dir match {
          case "r" => println("%d <- %d".format(num, renumberer.reverseTranslate(num)))
          case "f" => println("%d -> %d".format(num, renumberer.translate(num)))
        }

      }
      catch {
        case e: Exception => println("Invalid Input! Try again?")
      }
    }

  }
}
