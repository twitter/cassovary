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

import com.twitter.cassovary.graph.StoredGraphDir
import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.util.io.{FileReader, IoUtils, ListOfEdgesGraphReader}

// if args are specified they stand for: <name-of-dir> <name-of-graph>
object HelloLoadSharedGraph {

  def main(args: Array[String]) {
    val (dir, graphName, graphDirection) = if (args.length > 0) {
      val d = if (args.length >= 3) {
        args(2) match {
          case "BothInOut" => BothInOut
          case "OnlyOut" => OnlyOut
          case "OnlyIn" => OnlyIn
          case s => println(s"Unknown graph direction $s specified. Using OnlyOut !")
            OnlyOut
        }
      } else OnlyOut
      (args(0), args(1), d)
    }
    else {
      ("../cassovary-core/src/test/resources/graphs", "toy_list5edges", OnlyOut)
    }
    val sep = detectGraphFormat(dir, graphName)
    println(s"Will read graph $graphName from directory $dir using separator ${sep.toInt}")

    println(s"\nBuilding shared graph for direction $graphDirection")
    val graph = ListOfEdgesGraphReader.forIntIds(dir, graphName, separator = sep,
      graphDir = graphDirection).toSharedArrayBasedDirectedGraph()
    println(s"A SharedArrayBasedDirectedGraph with ${graph.nodeCount} nodes has ${graph.edgeCount} directed edges.")
  }

  private def detectGraphFormat(directory: String, prefixFileNames: String) = {
    val validFiles = IoUtils.readFileNames(directory, prefixFileNames)
    if (validFiles.isEmpty) {
      println(s"No file starting with $prefixFileNames found in $directory!")
      System.exit(-1)
    }
    val name = validFiles(0)
    val formatReader = new FileReader[Char](name) {
      def processOneLine(line: String) = {
        val index = line.indexWhere(c => c < '0' || c > '9')
        if (index == -1) {
          println(s"Unable to infer format type of file $name in directory $directory!")
          System.exit(-1)
          -1.toChar
        } else line(index)
      }
    }
    val separator = formatReader.next()
    formatReader.close()
    separator
  }

}
