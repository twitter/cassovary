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
package com.twitter.cassovary.server

import com.twitter.ostrich.admin.config.ServerConfig
import com.twitter.ostrich.admin.RuntimeEnvironment

/**
 * Companion to CachedDirectedGraphServer, required by Ostrich
 */
class CachedDirectedGraphServerConfig extends ServerConfig[CachedDirectedGraphServer] {
  var serverPort: Int = 9999

  var nodeList = "/path/to/nodelist"
  var verbose = false
  var graphDump = "/path/to/graphdump"
  var inGraphDump = "/path/to/inedgegraphdump"
  var cacheType = "lru"
  var numNodes: Int = 10
  var numEdges: Long = 100L
  var shardDirectories: Array[String] = Array("path/to/sharddirs")
  var numShards = 256
  var numRounds = 16
  var cacheDirectory = "/path/to/cachedir"
  var experiment = "ptc"
  var iterations = 5
  var outputDirectory = "/path/to/outputdir"

  def apply(runtime: RuntimeEnvironment) = {
    new CachedDirectedGraphServer(this)
  }
}
