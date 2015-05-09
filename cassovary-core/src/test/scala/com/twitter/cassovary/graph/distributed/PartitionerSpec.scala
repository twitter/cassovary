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

package com.twitter.cassovary.graph.distributed

import com.twitter.cassovary.graph._
import org.scalatest.{Matchers, WordSpec}

class PartitionerSpec extends WordSpec with Matchers with GraphBehaviours[Node] {

  val whole = TestGraphs.g6_onlyout

  // check if each edge of `orig` is present in some instance of the subgraphs
  // note that implementation is not efficient and is suitable for small tests only.
  def checkEdgesPresentInSomeInstance(orig: DirectedGraph[Node],
      instances: Map[Int, DirectedGraph[Node]]): Unit = {
    val subgraphs = instances.values
    orig foreach { node =>
      for (dir <- Seq(GraphDir.OutDir, GraphDir.InDir)) {
        node.neighborIds(dir).foreach { neighborId =>
          val exists = subgraphs exists { graph =>
            graph.getNodeById(node.id).flatMap(_.neighborIds(dir).find(_ == neighborId)).isDefined
          }
          exists shouldEqual true
        }
      }
    }
  }

  def checkEntireNodePresentInSomeInstance(orig: DirectedGraph[Node],
        instances: Map[Int, DirectedGraph[Node]]): Unit = {

    val subgraphs = instances.values
    orig foreach { origNode =>
      val origInNeighbors = origNode.neighborIds(GraphDir.InDir).toSet
      val origOutNeighbors = origNode.neighborIds(GraphDir.OutDir).toSet
      val foundSubGraph = subgraphs exists { subgraph =>
        val node = subgraph.getNodeById(origNode.id)
        node exists { n =>
          (n.neighborIds(GraphDir.InDir).toSet == origInNeighbors) &&
              (n.neighborIds(GraphDir.OutDir).toSet == origOutNeighbors)
        }
      }
      withClue("Node " + origNode.id + " was not found any subgraph") {
        foundSubGraph shouldEqual true
      }
    }
  }

  "Node-mapper type partitioners should retain each node" when {
    "RandomNodeMapper" in {
      val partitioner = new RandomNodeMapper(4)
      val instances = Partitioner.fromArrayBasedDirectedGraph(whole, partitioner)
      checkEntireNodePresentInSomeInstance(whole, instances)
    }
    "HashSourceMapper" in {
      val partitioner = new HashSourceMapper(5, i => i + 1)
      val instances = Partitioner.fromArrayBasedDirectedGraph(whole, partitioner)
      checkEntireNodePresentInSomeInstance(whole, instances)
    }
  }

  "Edge-mapper type partitioners should retain each edge" when {
    "HashDestMapper" in {
      val partitioner = new HashDestMapper(10, i => i + 1)
      val instances = Partitioner.fromArrayBasedDirectedGraph(whole, partitioner)
      checkEdgesPresentInSomeInstance(whole, instances)
    }
  }

  "HashSourceAndDestMapper" when {
    "mapped into many instances, should retain each edge in some instance" should {
      val partitioner = new HashSourceAndDestMapper(10, i => i + 1)
      val instances = Partitioner.fromArrayBasedDirectedGraph(whole, partitioner)
      checkEdgesPresentInSomeInstance(whole, instances)
    }

    val partitioner = new HashSourceAndDestMapper(2, i => i % 2)
    val instances = Partitioner.fromArrayBasedDirectedGraph(whole, partitioner)

    "when mapped to 2 instances, instance 0 (containing even nodes)" should {
      val outEdges0 = Map(
        10 -> Seq(11, 12, 13),
        11 -> Seq(12, 14),
        12 -> Seq(14),
        13 -> Seq(12, 14),
        14 -> Seq(15),
        15 -> Seq(10)
      )
      verifyInOutEdges(instances(0), whole.nodeCount, outEdges0, Map(), true)
    }

    "when mapped to 2 instances, instance 1 (containing odd nodes)" should {
      val outEdges1 = Map(
        10 -> Seq(11, 13),
        11 -> Seq(12, 14),
        13 -> Seq(12, 14),
        14 -> Seq(15),
        15 -> Seq(10, 11)
      )
      verifyInOutEdges(instances(1), whole.nodeCount, outEdges1, Map(), true)
    }
  }
}
