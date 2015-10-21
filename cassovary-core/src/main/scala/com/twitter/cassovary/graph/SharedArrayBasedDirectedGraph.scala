package com.twitter.cassovary.graph

import java.util.concurrent.atomic.AtomicInteger

import com.google.common.annotations.VisibleForTesting
import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.graph.node._
import com.twitter.cassovary.util.collections.Int2IntMap
import com.twitter.cassovary.util.{ArrayBackedSet, BoundedFuturePool, Sharded2dArray}
import com.twitter.finagle.stats.{Stat, DefaultStatsReceiver}
import com.twitter.logging.Logger
import com.twitter.util.{Await, Future, FuturePool}


object SharedArrayBasedDirectedGraph {
  private lazy val log = Logger.get()
  private val statsReceiver = DefaultStatsReceiver
  val emptyArray = Array.empty[Int]

  private object EdgeShards {
    private val numBits = 8
    val numShards = 1 << numBits
    private val mask = numShards - 1

    def hash(i: Int) = i & mask
  }

  /**
   * Construct a shared array-based graph from a sequence of NodeIdEdgesMaxId iterables.
   * Eg each Iterable[NodeIdEdgesMaxId] could come from one graph dump file.
   *
   * This function builds the graph using similar steps as in ArrayBasedDirectedGraph.
   * The main difference here is that instead of each node storing neighbor ids in
   * separate arrays of ids, here one shared array is used for all neighbor ids.
   * Thus each node can find its edges through an offset into this shared array.
   * To avoid huge arrays, this edge array is also sharded based on node's id.
   *
   * @param iterableSeq the sequence of nodes each with its own edges
   * @param parallelismLimit number of threads construction uses
   * @param storedGraphDir the direction of the graph to be built
   * @param forceSparseRepr if Some(true), the code saves storage at the expense of speed
   *                        by using ConcurrentHashMap instead of Array. If Some(false), chooses
   *                        Array instead. If None, the code calculates whether the graph
   *                        is sparse based on the number of nodes and maximum node id.
   */
  def apply(iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]], parallelismLimit: Int,
      storedGraphDir: StoredGraphDir, forceSparseRepr: Option[Boolean] = None) = {
    val constructor = new SharedArrayBasedDirectedGraphConstructor(iterableSeq, parallelismLimit,
      storedGraphDir, forceSparseRepr)
    constructor.construct()
  }

  private class SharedArrayBasedDirectedGraphConstructor(
    iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]],
    parallelismLimit: Int,
    storedGraphDir: StoredGraphDir,
    forceSparseRepr: Option[Boolean] = None) {

    private val futurePool = new BoundedFuturePool(FuturePool.unboundedPool, parallelismLimit)

    // info kept for each shard while building
    private class PerShardInfo {
      val edgeCount = new AtomicInteger()
      val nextFreeEdgeIndex = new AtomicInteger()
    }

    /**
     * For each shard, go over each NodeIdEdgesMaxId and calculate the size of this shard by
     * summing the edge lengths of all nodes that fall into this shard.
     * An edge falls in a shard when the source node id is hashed to the index of the shard.
     *
     * @return shared graph meta-information object with filled all information but node count
     */
    private def readMetaInfoPerShard(shardsInfo: Array[PerShardInfo]):
    Future[Seq[SharedGraphMetaInfo]] = {
      log.info("read out num of edges and max id from files in parallel")
      val stat = statsReceiver.stat("graph_load_read_out_edge_sizes_dump_files")
      Stat.timeFuture(stat) {
        val futures = iterableSeq map {
          edgesIterable => futurePool {
            var id, newMaxId, numOfEdges, edgesLength, numNodes = 0
            val iteratorForEdgeSizes = edgesIterable.iterator
            iteratorForEdgeSizes foreach { item =>
              id = item.id
              newMaxId = newMaxId max item.maxId
              edgesLength = item.edges.length
              // +1 because we will keep edgesLength in the very first index
              shardsInfo(EdgeShards.hash(id)).edgeCount.addAndGet(edgesLength + 1)
              numOfEdges += edgesLength
              numNodes += 1
            }
            SharedGraphMetaInfo(newMaxId, numOfEdges, numNodes)
          }
        }
        Future.collect(futures)
      }
    }

    /**
     * Aggregate meta-information from parts of graph
     */
    private def aggregateMetaInfoFromParts(partsMetaInfo: Seq[SharedGraphMetaInfo]):
      SharedGraphMetaInfo = {

      def aggregate(meta1: SharedGraphMetaInfo, meta2: SharedGraphMetaInfo) = {
        SharedGraphMetaInfo(meta1.maxId max meta2.maxId,
          meta1.edgeCount + meta2.edgeCount, meta1.numNodes + meta2.numNodes)
      }

      partsMetaInfo.reduce(aggregate)
    }

    /**
     * Instantiates shared array. Resets `sharedEdgeArraySizeCount`.
     */
    private def instantiateSharedArray(shardsInfo: Array[PerShardInfo]):
    Array[Array[Int]] = {
      // instantiate shared array (2-dimensional)
      val sharedEdgeArray = new Array[Array[Int]](EdgeShards.numShards)
      for (i <- 0 until EdgeShards.numShards) {
        sharedEdgeArray(i) = new Array[Int](shardsInfo(i).edgeCount.get)
      }
      sharedEdgeArray
    }

    private def fillEdgesMarkNodeOffsets(iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]],
                                         sharedEdgeArray: Array[Array[Int]],
                                         nodeCollection: NodeCollection,
                                         shardsInfo: Array[PerShardInfo],
                                         partsMetaInfo: Seq[SharedGraphMetaInfo]): Future[Unit] = {
      log.debug("loading nodes and out edges from file in parallel and marking all nodes")
      val allNodeIdsSet = new ArrayBackedSet(nodeCollection.maxNodeId)

      val futures = iterableSeq.indices.map {
        index => futurePool {
          val edgesIterable = iterableSeq(index)
          val ids = new Array[Int](partsMetaInfo(index).numNodes)
          val offsets = new Array[Int](partsMetaInfo(index).numNodes)
          var id, edgesLength, shardIdx, offset, i = 0
          edgesIterable.foreach { item =>
            id = item.id
            edgesLength = item.edges.length
            shardIdx = EdgeShards.hash(id)
            offset = shardsInfo(shardIdx).nextFreeEdgeIndex.getAndAdd(edgesLength + 1)
            Array.copy(item.edges, 0, sharedEdgeArray(shardIdx), offset + 1, edgesLength)
            ids(i) = id
            offsets(i) = offset + 1
            i += 1
            sharedEdgeArray(shardIdx)(offset) = edgesLength
            item.edges foreach { edge => allNodeIdsSet.add(edge) }
          }
          (ids, offsets)
        }
      }

      Future.collect(futures).map { idsAndOffsetsAll =>
        // serialize addition to nodeCollection
        // don't need to keep allNodeIdsSet around, encode in offsets()
        allNodeIdsSet.foreach { i => nodeCollection.updateOffset(i, 0) }
        idsAndOffsetsAll foreach { case (ids, offsets) =>
            ids.indices foreach { i => nodeCollection.updateOffset(ids(i), offsets(i)) }
        }
      }
    }

    private def sharded2dArray(nc: NodeCollection, sharedEdgeArray: Array[Array[Int]]) = {

      def numEdges(id: Int) = {
        if (nc.emptyNode(id)) 0
        else {
          sharedEdgeArray(EdgeShards.hash(id))(nc.getEdgeOffset(id) - 1)
        }
      }

      new Sharded2dArray[Int](sharedEdgeArray, nc.validNode, nc.offsets, numEdges, EdgeShards.hash)
    }


    def construct(): SharedArrayBasedDirectedGraph = {
      val shardsInfo = Array.fill(EdgeShards.numShards)(new PerShardInfo)

      val future = for {
        partsMetaInfo <- readMetaInfoPerShard(shardsInfo)
        metaInfo = aggregateMetaInfoFromParts(partsMetaInfo)
        nodeCollection = new NodeCollection(metaInfo, forceSparseRepr)
        sharedEdgeArray = instantiateSharedArray(shardsInfo)
        _ <- fillEdgesMarkNodeOffsets(iterableSeq, sharedEdgeArray, nodeCollection,
          shardsInfo, partsMetaInfo)
        outEdges = sharded2dArray(nodeCollection, sharedEdgeArray)
        reverseDirEdgeArray <-
          if (storedGraphDir == BothInOut)
            createReverseDirEdgeArray(outEdges, nodeCollection)
          else
            Future.value(None)
      } yield new SharedArrayBasedDirectedGraph(nodeCollection, outEdges,
          reverseDirEdgeArray, metaInfo, storedGraphDir)

      Await.result(future)
    }

    /**
     * Creates an array for reverse direction edges.
     *
     * Needed only if storing both directions.
     */
    private def createReverseDirEdgeArray(outEdges: Sharded2dArray[Int], nodeCollection: NodeCollection):
    Future[Option[Sharded2dArray[Int]]] = {

      val numNodes = nodeCollection.size
      val nodesWithInEdges = new NodeCollection(nodeCollection.graphInfo, forceSparseRepr)
      val inEdgesSizes = new Array[AtomicInteger](nodeCollection.maxNodeId + 1)
      val reverseShardsInfo = Array.fill(EdgeShards.numShards)(new PerShardInfo)

      // do function f for all nodes, divided into EdgeShards.numShards futures
      def doForAllNodeIds(f: Int => Unit): Future[Unit] = {
        val grouped = nodeCollection.iterator.grouped(1 + numNodes/EdgeShards.numShards)
        val futures = grouped.toSeq map { ids =>
          futurePool { ids foreach f }
        }
        Future.join(futures)
      }

      def findInEdgesSizes() = {
        log.info("calculating incoming neighbor sizes for each node")
        Stat.timeFuture(statsReceiver.stat("graph_load_find_in_edge_shards_sizes")) {
          nodeCollection foreach { id =>
            inEdgesSizes(id) = new AtomicInteger()
          }
          doForAllNodeIds { id: Int =>
            outEdges(id) foreach { neighbor =>
              inEdgesSizes(neighbor).incrementAndGet()
            }
          }
        }
      }

      def findInShardSizes() = {
        log.info("calculating in shard sizes")
        // one more pass to adjust per-shard array sizes and also update offsets
        doForAllNodeIds { id: Int =>
          val len = inEdgesSizes(id).get
          val shard = EdgeShards.hash(id)
          val offset = if (len > 0) {
            reverseShardsInfo(shard).edgeCount.getAndAdd(1 + len) + 1
          } else 0
          nodesWithInEdges.updateOffset(id, offset)
        }
      }

      def fillInEdgesOffsets(sharedInEdgesArray: Array[Array[Int]]): Future[Unit] = {
        log.info("filling lengths and offsets")
        Stat.timeFuture(statsReceiver.stat("graph_load_fill_in_edge_lengths_and_offsets")) {
          doForAllNodeIds { id =>
            val len = inEdgesSizes(id).get
            if (len > 0) {
              val shard = EdgeShards.hash(id)
              val off = nodesWithInEdges.offsets(id)
              sharedInEdgesArray(shard)(off - 1) = len
              inEdgesSizes(id).set(off) // we will start storing actual neighbors starting here
            }
          }
        }
      }

      def fillInEdges(sharedInEdgesArray: Array[Array[Int]],
                      nextFreeEdgeIndexPerNode: Array[AtomicInteger]): Future[Unit] = {
        log.info("filling in edges")
        val fut = Stat.timeFuture(statsReceiver.stat("graph_load_fill_in_edges")) {
          doForAllNodeIds { nodeId =>
            outEdges(nodeId) foreach { neighborId =>
              val shard = sharedInEdgesArray(EdgeShards.hash(neighborId))
              shard(nextFreeEdgeIndexPerNode(neighborId).getAndIncrement) = nodeId
            }
          }
        }
        log.info("DONE filling in edges")
        fut
      }

      // main set of steps to build incoming edges in the graph
      log.info("Now building all the incoming edges")
      for {
        _ <- findInEdgesSizes()
        _ <- findInShardSizes()
        sharedInEdges = instantiateSharedArray(reverseShardsInfo)
        _ <- fillInEdgesOffsets(sharedInEdges)
        _ <- fillInEdges(sharedInEdges, inEdgesSizes)
      } yield Some(sharded2dArray(nodesWithInEdges, sharedInEdges))
    }
  }

  @VisibleForTesting
  def apply(iterable: Iterable[NodeIdEdgesMaxId], storedGraphDir: StoredGraphDir):
    SharedArrayBasedDirectedGraph = apply(Seq(iterable), 1, storedGraphDir)
}

private class NodeCollection(val graphInfo: SharedGraphMetaInfo, forceSparsity: Option[Boolean] = None)
    extends Iterable[Int] {

  val maxNodeId = graphInfo.maxId
  private val considerGraphSparse: Boolean = forceSparsity getOrElse {
    // sparse if number of nodes is much less than maxNodeId, AND
    // number of edges is also less than maxNodeId. If number of edges
    // were similar to or greater than maxNodeId, then the extra overhead of allocating
    // an array of size maxNodeId is not too much relative to the storage occupied by
    // the edges themselves
    (graphInfo.numNodes * 8 < maxNodeId) && (graphInfo.edgeCount < maxNodeId)
  }

  /**
   * Encoding of node with id=i with just one array `offsets` of ints:
   * 1. i exists in the graph. edgeOffsets(i) > 0 and length(i) = arr(edgeOffsets(i)-1).
   *    Edges(i) are in arr[edgeOffsets(i)..edgeOffsets(i)+length(i)-1]
   * 2. i is an empty node with no edges. edgeOffsets(i) == 0
   * (Obsoleted when we are not using a full array: 3. i does not exist in the graph. edgeOffsets(i) == -1)
   */
  //val edgeOffsets = Array.fill[Int](maxNodeId + 1)(-1)
  private val edgeOffsets = Int2IntMap(isSparse = considerGraphSparse, numKeysEstimate = None, maxId = Some(maxNodeId))

  def offsets: Int => Int = edgeOffsets

  def updateOffset(i: Int, v: Int) { edgeOffsets(i) = v }
  def getEdgeOffset(id: Int) = edgeOffsets(id)

  def validNode(id: Int) = edgeOffsets.contains(id)
  def hasEdges(id: Int) = validNode(id) && edgeOffsets(id) > 0
  def emptyNode(id: Int) = validNode(id) && edgeOffsets(id) == 0

  // iterator of valid node ids
  def iterator = edgeOffsets.keysIterator

  override lazy val size = edgeOffsets.size
}


/**
 * Contains meta-information about a particular graph instance.
 *
 * @param maxId the max node id in the graph
 * @param edgeCount the number of edges in the graph
 */
case class SharedGraphMetaInfo(maxId: Int, edgeCount: Long, numNodes: Int)

/**
 * This class is an implementation of the directed graph trait that is backed
 * by a sharded 2-dimensional edges array. Each node's edges are stored
 * consecutively in one shard of the edge array. Number of shard is usually much
 * smaller than number of nodes.
 *
 * @param edges the 2-dimensional sharded edge array
 * @param reverseDirEdges the reverse edge array, one per node
 * @param metaInformation graph meta-information
 */
class SharedArrayBasedDirectedGraph private (
  nodeCollection: NodeCollection,
  edges: Sharded2dArray[Int],
  reverseDirEdges: Option[Sharded2dArray[Int]],
  metaInformation: SharedGraphMetaInfo,
  val storedGraphDir: StoredGraphDir
) extends DirectedGraph[Node] {

  lazy val nodeCount: Int = nodeCollection.size

  lazy val edgeCount: Long = metaInformation.edgeCount

  override lazy val maxNodeId = nodeCollection.maxNodeId

  private def newNode(id: Int) = SharedArrayBasedDirectedNode(id,
          edges, storedGraphDir, reverseDirEdges)

  def iterator = nodeCollection.iterator.map(newNode)

  def getNodeById(id: Int): Option[Node] = {
    if ((id < 0) || (id > maxNodeId) && !nodeCollection.validNode(id)) {
      None
    } else {
      Some(newNode(id))
    }
  }
}
