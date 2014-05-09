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
package com.twitter.cassovary.graph2

import com.twitter.util.{Await, Future}

/**
 * This is an alternative base trait for a graph. It allows you to keep
 * distinct information about the source nodes, destination nodes and the edges
 *
 * Please consider this experimental and subject to change.
 *
 * Here are the properties
 * - The trait implements an adjacency representation from source nodes to
 *   destination nodes
 * - Given a destination node, we need not have the incoming nodes
 * - All nodes are indexed by a long id
 * - All source nodes have a unique id
 * - All destination nodes have a unique id
 * - The sets of source node ids and destination node ids are not necessarily
 *   disjoint
 * - We cannot determine the incoming edges for a node V directly. If this is
 *   needed, it can be arranged by additionally maintaining the transpose of
 *   this graph
 *
 * The default implementations for some of the methods may be inefficient.
 *
 * @tparam U the type of the source nodes
 * @tparam V the type of the destination nodes
 * @tparam E the type of the edge
 */
trait Graph2[+U, +V, +E] {
  /**
   * @param u source node id
   * @returns the source node object, given its index
   */
  def sourceNode(u: Long): Future[Option[U]]

  /**
   * @param v destination node id
   * @returns the destination node object, given its index
   */
  def destinationNode(v: Long): Future[Option[V]]

  /**
   * Returns the destination node ids and the edge object corresponding to each edge
   * @param u source node id
   * @returns outbound edge objects, and the destination ids
   */
  def edges(u: Long): Future[Option[Seq[(Long, E)]]]

  /**
   * Returns the destination node ids for a given source id
   * @param u source node id
   * @returns node ids of the neighbors of the input node
   */
  def neighbors(u: Long): Future[Option[Seq[Long]]] = edges(u) map { _ map { _ map { case (id, _) => id } } }
}

/**
 * This is a simpler version of Graph2, where there is no notion of edge or node types
 */
trait SimpleGraph2 extends Graph2[Long, Long, Unit] {
  def sourceNode(u: Long): Future[Option[Long]] = Future.value(Some(u))
  def destinationNode(v: Long): Future[Option[Long]] = Future.value(Some(v))
  def edges(u: Long): Future[Option[Seq[(Long, Unit)]]] = neighbors(u) map { _ map { _ map { id => (id, ()) } } }
}
