package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.node.DynamicNode
import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.util.FastUtilUtils
import it.unimi.dsi.fastutil.ints.IntArrayList
import scala.collection.mutable.ArrayBuffer

/**
 * A dynamic directed graph, implemented using an ArrayBuffer of IntArrayList
 * (from the fastutil library).  If n nodes are used, O(n) objects are created,
 * independent of the number of edges.
 *
 * For efficiency, it's recommended that ids be sequentially numbered, as
 * an internal array is stored which is longer than the maximum id seen.
 *
 * This class is not threadsafe.  If multiple thread access it simultaneously
 * and at least one mutates it, synchronization should be used.  Currently
 * no new threads are created by this class (all operations execute on a single thread).
 */
class ArrayBasedDynamicDirectedGraph(val storedGraphDir: StoredGraphDir)
    extends DynamicDirectedGraph[DynamicNode] {
  // outboundLists(id) contains the outbound neighbors of the given id,
  // or null if the id is not in this graph.
  // If we aren't storing outbound neighbors, outboundLists will always remain size 0.
  private val outboundLists = new ArrayBuffer[IntArrayList]
  // (See above note on outboundLists)
  private val inboundLists = new ArrayBuffer[IntArrayList]

  private var _nodeCount = 0

  def this(dataIterable: Iterable[NodeIdEdgesMaxId],
            storedGraphDir: StoredGraphDir) {
    this(storedGraphDir)
    for (nodeData <- dataIterable.iterator) {
      val id = nodeData.id
      getOrCreateNode(id)
      nodeData.edges map getOrCreateNode
      storedGraphDir match {
        case OnlyOut | Mutual => outboundLists(id).addAll(IntArrayList.wrap(nodeData.edges))
        case OnlyIn => inboundLists(id).addAll(IntArrayList.wrap(nodeData.edges))
        case BothInOut => nodeData.edges map { addEdgeAllowingDuplicates(id, _) } // Duplicates shouldn't exist, but allow them for efficiency.
      }
    }
  }

  def this(iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]],
           storedGraphDir: StoredGraphDir) {
    this(iterableSeq.flatten , storedGraphDir)
  }

  /* Returns an option which is non-empty if outbound list for id  is non-null. */
  private def outboundListOption(id: Int): Option[IntArrayList] =
    if (id < 0 || id >= outboundLists.size)
      None
  else
      Option(outboundLists(id)) // If list is null, convert to None

  private def inboundListOption(id: Int): Option[IntArrayList] =
    if (id < 0 || id >= inboundLists.size)
      None
    else
      Option(inboundLists(id)) // If list is null, convert to None

  /** Wraps outboundList and inboundList in a node structure.  This is an inner class
    * because mutations will affect other nodes of the graph if both inbound
    * and outbound neighbor lists are stored.
    *
    * For efficiency, we don't store Nodes, but create them on the fly as needed.
    */
  class IntListNode(override val id: Int,
                    val outboundList: Option[Seq[Int]],
                    val inboundList: Option[Seq[Int]])
      extends DynamicNode {
    override def outboundNodes(): Seq[Int] = outboundList.getOrElse(Seq.empty)

    override def inboundNodes(): Seq[Int] = storedGraphDir match {
      case Mutual => outboundNodes()
      case _ => inboundList.getOrElse(Seq.empty)
      }

    def addOutBoundNodes(nodeIds: Seq[Int]): Unit = {
      //For future optimization, we could check if we are only storing outbound
      // nodes and then do:
      // outboundList.get.addAll(IntArrayList.wrap(nodeIds.toArray))
      nodeIds map { addEdge(id, _) }
    }

    def addInBoundNodes(nodeIds: Seq[Int]): Unit = {
      nodeIds map { addEdge(_, id) }
    }

    def removeOutBoundNode(nodeId: Int): Unit = removeEdge(id, nodeId)

    def removeInBoundNode(nodeId: Int): Unit =  removeEdge(nodeId, id)
  }

  /**
   * Determine if a node with the given id exists in this graph.
   */
  def nodeExists(id: Int): Boolean =
    outboundListOption(id).nonEmpty || inboundListOption(id).nonEmpty

  /**
   * Returns the node with the given {@code id} or else {@code None} if the given node does not
   * exist in this graph.
   */
  override def getNodeById(id: Int): Option[DynamicNode] =
    if (nodeExists(id)) {
      Some(new IntListNode(id,
        outboundListOption(id) map FastUtilUtils.intArrayListToSeq,
        inboundListOption(id) map FastUtilUtils.intArrayListToSeq))
    } else {
      None
    }

  override def iterator: Iterator[DynamicNode] = (0 until maxIdBound).iterator flatMap getNodeById

  /**
   * Returns the total number of directed edges in the graph.  A mutual edge, eg: A -> B and B -> A,
   * counts as 2 edges in this total.
   */
  override def edgeCount: Long =
    if (StoredGraphDir.isOutDirStored(storedGraphDir))
      (iterator map {_.outboundCount }).sum
    else
      (iterator map {_.inboundCount }).sum

  /**
   * Returns the number of nodes in the graph.
   */
  override def nodeCount: Int = _nodeCount
  // Or this can be computed as (0 until maxIdBound) count nodeExists

  /**
   * Remove an edge from a {@code srdId} to {@code destId}.
   * Return Option of source and destination nodes. None indicates the node doesn't exist in graph.
   */
  override def removeEdge(srcId: Int, destId: Int): (Option[Node], Option[Node]) = {
    outboundListOption(srcId) map {list => list.rem(destId)}
    inboundListOption(destId) map {list => list.rem(srcId)}
    if (storedGraphDir == Mutual) {
      outboundListOption(destId) map {list => list.rem(srcId)}
    }
    (getNodeById(srcId), getNodeById(destId))
  }

  /**
   * Add an edge from {@code srcId} to {@code destId}.
   * If the edge already exists, nothing will change. This takes time proportional
   * to out-degree(srcId) + in-degree(destId).
   */
  override def addEdge(srcId: Int, destId: Int): Unit = {
    getOrCreateNode(srcId)
    getOrCreateNode(destId)
    def addIfMissing(list: IntArrayList, k: Int): Unit =
      if (!list.contains(k))
        list.add(k)
    outboundListOption(srcId) map { addIfMissing(_, destId) }
    inboundListOption(destId) map { addIfMissing(_, srcId) }
    if (storedGraphDir == StoredGraphDir.Mutual) {
      // Add mutual edge also
      outboundListOption(destId) map { addIfMissing(_, srcId) }
    }
  }

  /**
   * Add an edge from {@code srcId} to {@code destId}.
   * If the edge already exists, this will create a parallel edge.  This runs in
   * constant amortized time.
   */
  def addEdgeAllowingDuplicates(srcId: Int, destId: Int): Unit = {
    getOrCreateNode(srcId)
    getOrCreateNode(destId)

    outboundListOption(srcId) map { _.add(destId) }
    inboundListOption(destId) map { _.add(srcId) }
    if (storedGraphDir == StoredGraphDir.Mutual) {
      // Add mutual edge also
      outboundListOption(destId) map { _.add(srcId) }
    }
  }

  /**
   * Add a node {@code id} into the graph.
   */
  override def getOrCreateNode(id: Int): DynamicNode = {
    def addIdToList(list: ArrayBuffer[IntArrayList]) {
      if (list.size <= id) {
        list.appendAll(Array.fill(id - list.size + 1 )(null))
      }
      list(id) = new IntArrayList(initialNeighborListCapacity)
    }

    if (!nodeExists(id)) {
      _nodeCount += 1
      if (StoredGraphDir.isOutDirStored(storedGraphDir)) {
        addIdToList(outboundLists)
      }
      if (StoredGraphDir.isInDirStored(storedGraphDir) &&
        storedGraphDir != StoredGraphDir.Mutual) {
        addIdToList(inboundLists)
      }
    }

    getNodeById(id).get
  }
  
  
  private def maxIdBound: Int = math.max(outboundLists.size,
                                         inboundLists.size)

  // This can be overridden if a user knows there will be many nodes with no
  // neighbors, or if most nodes will have many more than 4 neighbors
  protected def initialNeighborListCapacity: Int = 4
}
