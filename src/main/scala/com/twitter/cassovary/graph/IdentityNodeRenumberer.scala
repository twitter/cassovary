/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.cassovary.graph

/*
 * Performs no node renumbering: simply echoes the passed node id or idx to the caller.
 */
class IdentityNodeRenumberer extends NodeRenumberer {
  
  def nodeIdToNodeIdx(nodeId: Int): Int = nodeId

  def nodeIdxToNodeId(nodeIdx: Int): Int = nodeIdx

}

