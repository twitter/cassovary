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
package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.StoredGraphDir._
import java.util.concurrent.{Future, ExecutorService}
import node._
import com.google.common.annotations.VisibleForTesting
import com.twitter.ostrich.stats.Stats
import java.util.concurrent.atomic.AtomicLong
import com.google.common.util.concurrent.MoreExecutors
import java.io._
import collection.mutable
import net.lag.logging.Logger
import com.google.common.cache.{LoadingCache, CacheLoader, Weigher, CacheBuilder}
import com.twitter.cassovary.util._
import cache._
import scala.Some

private case class MaxIdsEdges(localMaxId:Int, localNodeWithoutOutEdgesMaxId:Int, numEdges:Int, nodeCount:Int)

/**
 * Methods for constructing a disk-cached directed graph
 */
object CachedDirectedGraph {

  private lazy val log = Logger.get("CachedDirectedGraph")

  def getMaxCounts(iteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ],
                   executorService: ExecutorService,
                   serializer: LoaderSerializer, name: String) = {
    var maxId, nodeWithXEdgesMaxId, nodeWithXEdgesCount = 0
    var numEdges = 0L
    serializer.writeOrRead(name, { writer =>
      val futures = Stats.time("graph_load_reading_maxid_and_calculating_numedges") {
        def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) = {
          var localMaxId, localNodeWithOutEdgesMaxId, numEdges, nodeCount = 0
          iteratorFunc().foreach { item =>
          // Keep track of Max IDs
            localMaxId = localMaxId max item.maxId
            localNodeWithOutEdgesMaxId = localNodeWithOutEdgesMaxId max item.id
            // Update nodeCount and total edges
            numEdges += item.edges.length
            nodeCount += 1
          }
          MaxIdsEdges(localMaxId, localNodeWithOutEdgesMaxId, numEdges, nodeCount)
        }
        ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], MaxIdsEdges](executorService,
          iteratorSeq, readOutEdges)
      }
      futures.toArray map { future =>
        val f = future.asInstanceOf[Future[MaxIdsEdges]]
        val MaxIdsEdges(localMaxId, localNWOEMaxId, localNumEdges, localNodeCount) = f.get
        maxId = maxId max localMaxId
        nodeWithXEdgesMaxId = nodeWithXEdgesMaxId max localNWOEMaxId
        numEdges += localNumEdges
        nodeWithXEdgesCount += localNodeCount
      }
      writer.integers(Seq(maxId, nodeWithXEdgesMaxId, nodeWithXEdgesCount)).longs(Seq(numEdges)).close
    }, { reader =>
      maxId = reader.int
      nodeWithXEdgesMaxId = reader.int
      nodeWithXEdgesCount = reader.int
      numEdges = reader.long
      reader.close
    })
    (maxId, nodeWithXEdgesMaxId, nodeWithXEdgesCount, numEdges)
  }

  def generateOffsetTablesForIn(iteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ],
                                executorService: ExecutorService,
                                serializer: LoaderSerializer, name: String, numShards: Int,
                                inArraySize: Int, ren: Renumberer = null) = {
    var idToIntOffset: Array[Long] = null
    var idToNumEdges: Array[Int] = null
    var edgeOffsets: Array[AtomicLong] = null

    serializer.writeOrRead(name, { writer =>
      idToIntOffset = new Array[Long](inArraySize+1)
      idToNumEdges = new Array[Int](inArraySize+1)
      edgeOffsets = new Array[AtomicLong](numShards)
      (0 until numShards).foreach { i => edgeOffsets(i) = new AtomicLong() }
      Stats.time("graph_load_generating_offset_tables") {

        if (ren == null) {
          def readInEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) {
            iteratorFunc() foreach { item =>
              val id = item.id
              val itemEdges = item.edges.length
              // Store offsets
              val edgeOffset = edgeOffsets(id % numShards).getAndAdd(itemEdges)
              idToNumEdges(id) = itemEdges
              idToIntOffset(id) = edgeOffset
            }
          }
          ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
            iteratorSeq, readInEdges).toArray.map { future => future.asInstanceOf[Future[Unit]].get }
        }
        else {
          def readInEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) {
            iteratorFunc() foreach { item =>
              val id = ren.translate(item.id)
              val itemEdges = item.edges.length
              item.edges foreach { edge => ren.translate(edge) }
              // Store offsets
              val edgeOffset = edgeOffsets(id % numShards).getAndAdd(itemEdges)
              idToNumEdges(id) = itemEdges
              idToIntOffset(id) = edgeOffset
            }
          }
          ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
            iteratorSeq, readInEdges).toArray.map { future => future.asInstanceOf[Future[Unit]].get }
        }
      }

      writer.arrayOfLong(idToIntOffset)
        .arrayOfInt(idToNumEdges)
        .atomicLongArray(edgeOffsets).close
    }, { reader =>
      idToIntOffset = reader.arrayOfLong()
      idToNumEdges = reader.arrayOfInt()
      edgeOffsets = reader.atomicLongArray()
      reader.close
    })
    (idToIntOffset, idToNumEdges, edgeOffsets)
  }

  def allocateShards(serializer: LoaderSerializer, name: String, shardDirectories: Array[String],
                     numShards: Int, edgeOffsets: Array[AtomicLong]) = {
    serializer.writeOrRead(name, { writer =>
      Stats.time("graph_load_allocating_shards") {
        val esw = new MultiDirIntShardsWriter(shardDirectories, numShards)
        (0 until numShards).foreach { i =>
          esw.shardWriters(i).allocate(edgeOffsets(i).get)
        }
        esw.close
      }
      writer.integers(Seq(1)).close
    }, { reader => reader.close })
  }

  def writeShardsInRounds(iteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ],
                          executorService: ExecutorService,
                          serializer: LoaderSerializer, name: String, shardDirectories: Array[String],
                          numShards: Int, numRounds: Int, edgeOffsets: Array[AtomicLong],
                          idToIntOffset: Array[Long], idToNumEdges: Array[Int]) = {
    serializer.writeOrRead(name, { writer =>
      val shardSizes = edgeOffsets.map { i => i.get().toInt }
      val msw = new MultiDirMemIntShardsWriter(shardDirectories, numShards, shardSizes, numRounds)
      (0 until numRounds).foreach { roundNo =>
        log.info("Beginning round %s...".format(roundNo))
        msw.startRound(roundNo)
        val (modStart, modEnd) = msw.roundRange
        Stats.time("graph_load_writing_round_to_shards") {
          def readInEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) {
            iteratorFunc() foreach { item =>
              val id = item.id
              val shardId = id % numShards
              if (modStart <= shardId && shardId < modEnd) {
                val edgeOffset = idToIntOffset(id)
                val numEdges = idToNumEdges(id)
                msw.writeIntegersAtOffsetFromOffset(id, edgeOffset.toInt, item.edges, 0, numEdges)
              }
            }
          }
          ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
            iteratorSeq, readInEdges).toArray.map { future => future.asInstanceOf[Future[Unit]].get }
        }
        log.info("Ending round %s...".format(roundNo))
        msw.endRound
      }
      writer.integers(Seq(1)).close
    }, { reader => reader.close })
  }

  def writeShardsInRoundsRenumbered(iteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ],
                                    executorService: ExecutorService,
                                    serializer: LoaderSerializer, name: String, shardDirectories: Array[String],
                                    numShards: Int, numRounds: Int, edgeOffsets: Array[AtomicLong],
                                    idToIntOffset: Array[Long], idToNumEdges: Array[Int], ren: Renumberer) = {
    // TODO maxSize of a shard is MAXINT * 4 bytes for now, not MAXLONG
    serializer.writeOrRead(name, { writer =>
      val shardSizes = edgeOffsets.map { i => i.get().toInt }
      val msw = new MultiDirMemIntShardsWriter(shardDirectories, numShards, shardSizes, numRounds)
      (0 until numRounds).foreach { roundNo =>
        log.info("Beginning round %s...".format(roundNo))
        msw.startRound(roundNo)
        val (modStart, modEnd) = msw.roundRange
        Stats.time("graph_load_writing_round_to_shards") {
          def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) {
            iteratorFunc() foreach { item =>
              val id = ren.translate(item.id)
              val shardId = id % numShards
              if (modStart <= shardId && shardId < modEnd) {
                val edgeOffset = idToIntOffset(id)
                val numEdges = idToNumEdges(id)
                msw.writeIntegersAtOffsetFromOffset(id, edgeOffset.toInt, ren.translateArray(item.edges), 0, numEdges)
              }
            }
          }
          ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
            iteratorSeq, readOutEdges).toArray.map { future => future.asInstanceOf[Future[Unit]].get }
        }
        log.info("Ending round %s...".format(roundNo))
        msw.endRound
      }
      writer.integers(Seq(1)).close

      log.info("Writing renumberer after writeShardsInRoundsRenumbered...")
      serializer.writeOrRead(name + ".ren", { writer =>
        ren.toWriter(writer)
        writer.close
      }, { reader => reader.close })

    }, { reader => reader.close })
  }

  /**
   * Make a new CachedDirectedGraph
   * TODO this is confusing now, because outIteratorSeq can be the in-edge sequence if inIteratorSeq is not provided.
   * TODO in fact, outIteratorSeq should be called "firstIteratorSeq", inIteratorSequence called "secondIteratorSeq"
   * TODO and storedGraphDir should be called "firstIteratorSeqDirection"
   * @param outIteratorSeq
   * @param inIteratorSeq
   * @param executorService
   * @param storedGraphDir
   * @param cacheType What kind of cache do you want to use? (lru, lru_na, clock, clock_na, guava)
   * @param cacheMaxNodes How many nodes should the cache store (at most)?
   * @param cacheMaxEdges How many edges should the cache store (at most)?
   * @param shardDirectoryPrefixes Where do you want generated shards to live?
   * @param numShards How many shards to generate?
   * @param numRounds How many rounds to generate the shards in? More rounds => less memory but takes longer
   * @param useCachedValues Reload everything? (i.e. ignore any cached objects).
   * @param cacheDirectory Where should the cached objects live?
   * @param renumber Do you want the nodes to be renumbered in increasing order? (Also reduces memory consumption)
   * @return an appropriate subclass of CachedDirectedGraph
   */
  def apply(outIteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ],
            inIteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ],
            executorService: ExecutorService,
            storedGraphDir: StoredGraphDir, cacheType: String, cacheMaxNodes:Int, cacheMaxEdges:Long,
            shardDirectoryPrefixes: Array[String], inShardDirectoryPrefixes: Array[String], numShards: Int, numRounds: Int,
            useCachedValues: Boolean, cacheDirectory: String, renumber: Boolean):CachedDirectedGraph = {

    // Initialize shard directory names
    val shardDirectories = shardDirectoryPrefixes.map ( shardDirectoryPrefix =>
      if (renumber) shardDirectoryPrefix + "_renumbered" else shardDirectoryPrefix + "_regular" )
    val shardDirectoriesIn = if (inShardDirectoryPrefixes != null && inShardDirectoryPrefixes(0).length > 0) {
      inShardDirectoryPrefixes.map ( shardDirectoryPrefix =>
        if (renumber) shardDirectoryPrefix + "_renumbered" else shardDirectoryPrefix + "_regular" )
    } else {
      shardDirectories.map { shardDirectory => shardDirectory + "_in" }
    }

    log.info("---Beginning Load---")
    log.info("Cache Info: cacheMaxNodes is %s, cacheMaxEdges is %s, cacheType is %s".format(cacheMaxNodes, cacheMaxEdges, cacheType))
    log.info("Disk Shard Info: directory is %s, numShards is %s, numRounds is %s".format(shardDirectories.deep.mkString(", "), numShards, numRounds))
    log.info("useCachedValues is %s, cacheDirectory is %s".format(useCachedValues, cacheDirectory))

    // Step 0 - Initialize serializer
    val serializer = new LoaderSerializer(cacheDirectory, useCachedValues)

    // Step 1
    // Find maxId, nodeWithOutEdgesMaxId, numInts
    // Needed so that we can initialize arrays with the appropriate sizes
    var maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount = 0
    var inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount = 0
    var numEdges, inNumEdges = 0L
    log.info("Reading maxId and Calculating numInts...")
    val oc = getMaxCounts(outIteratorSeq, executorService, serializer, "step1.txt")
    maxId = oc._1; nodeWithOutEdgesMaxId = oc._2; nodeWithOutEdgesCount = oc._3; numEdges = oc._4
    // Also do this for inIteratorSeq if it exists
    if (inIteratorSeq != null) {
      log.info("Reading maxId and Calculating numInts for in-edges...")
      val ic = getMaxCounts(inIteratorSeq, executorService, serializer, "step1in.txt")
      inMaxId = ic._1; nodeWithInEdgesMaxId = ic._2; nodeWithInEdgesCount = ic._3; inNumEdges = ic._4
    }

    var nodeIdSet: mutable.BitSet = null
    var idToIntOffset: Array[Long] = null
    var idToNumEdges: Array[Int] = null
    var edgeOffsets: Array[AtomicLong] = null
    var numNodes = 0

    var idToIntOffsetIn: Array[Long] = null
    var idToNumEdgesIn: Array[Int] = null
    var edgeOffsetsIn: Array[AtomicLong] = null

    /**
     * Steps 2 to 4/5 change depending on whether we need to renumber
     * Renumbering has steps 2r - 4r
     * Non-renumbering has steps 2 - 5
     */
    if (renumber) {
      log.info("Renumbering...")

      // Step 2
      // Renumber the out edges, then
      // Generate the lookup table for nodeIdSet
      // Generate and store shard offsets and # of edges for each node
      log.info("Renumbering nodes, generating offsets...")
      var ren = new Renumberer(maxId max inMaxId) // Must be able to translate both in and out
      serializer.writeOrRead("step2xr.txt", { writer =>

        log.info("First renumbering only nodes with out-edges...")
        val futures = Stats.time("graph_load_renumbering_nodes_with_outedges") {
          def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) = {
            iteratorFunc().foreach { item =>
              ren.translate(item.id)
            }
          }
          ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
            outIteratorSeq, readOutEdges)
        }
        futures.toArray map { future =>
          future.asInstanceOf[Future[Unit]].get
        }
        val outNodeCount = ren.count
        assert(outNodeCount == nodeWithOutEdgesCount)

        log.info("Then generating offset tables (renumbered)...")
        idToIntOffset = new Array[Long](outNodeCount+1)
        idToNumEdges = new Array[Int](outNodeCount+1)
        edgeOffsets = new Array[AtomicLong](numShards)
        (0 until numShards).foreach { i => edgeOffsets(i) = new AtomicLong() }
        Stats.time("graph_load_generating_offset_tables") {
          def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) {
            iteratorFunc() foreach { item =>
              val id = ren.translate(item.id)
              val itemEdges = item.edges.length
              item.edges foreach { edge => ren.translate(edge) }
              // Store offsets
              val edgeOffset = edgeOffsets(id % numShards).getAndAdd(itemEdges)
              idToNumEdges(id) = itemEdges
              idToIntOffset(id) = edgeOffset
            }
          }
          ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
            outIteratorSeq, readOutEdges).toArray.map { future => future.asInstanceOf[Future[Unit]].get }
        }

        // RESET maxId
        numNodes = ren.count

        // Step 2.1
        log.info("Saving/loading total number of nodes...")
        serializer.writeOrRead("step2xr1.txt", { writer =>
          writer.int(numNodes).close
        }, { reader => reader.close })

        // Step 2.2
        log.info("Saving/loading renumberer...")
        serializer.writeOrRead("step2xr2.txt", { writer =>
          ren.toWriter(writer)
          writer.close
        }, { reader => reader.close })

        log.info("Writing offset tables to file...")
        writer.arrayOfLong(idToIntOffset)
          .arrayOfInt(idToNumEdges)
          .atomicLongArray(edgeOffsets).close
      }, { reader =>

        // Step 2.1
        log.info("Saving/loading total number of nodes...")
        serializer.writeOrRead("step2xr1.txt", { writer => throw new Exception("2.1 Writer Impossible!")
        }, { reader =>
          numNodes = reader.int
          reader.close
        })

        // Step 2.2
        log.info("Saving/loading renumberer...")
        serializer.writeOrRead("step2xr2.txt", { writer => throw new Exception("2.2 Writer Impossible!")
        }, { reader =>
          // Don't load renumberer if we've already completed step 4 (and i3r maybe)
          if (!serializer.exists("step4r.txt") ||
            (inIteratorSeq != null && !serializer.exists("stepi3r.txt")))
            ren.fromReader(reader)
          else
            ren = null
          reader.close
        })

        idToIntOffset = reader.arrayOfLong()
        idToNumEdges = reader.arrayOfInt()
        edgeOffsets = reader.atomicLongArray()
        reader.close
      })

      // Step 3 - Allocating shards
      log.info("Allocating shards (renumbered)...")
      allocateShards(serializer, "step3r.txt", shardDirectories, numShards, edgeOffsets)

      // Step 4r - Generate shards on disk in rounds
      log.info("Writing to shards in rounds (renumbered)...")
      writeShardsInRoundsRenumbered(outIteratorSeq, executorService, serializer, "step4r.txt", shardDirectories,
        numShards, numRounds, edgeOffsets, idToIntOffset, idToNumEdges, ren)

      if (inIteratorSeq != null) {
        // Step i1r Generate Offset Table Counts
        log.info("Generating offset tables for in-shards (renumbered)...")
        // TODO to avoid ArrayOutOfBounds inArraySize is the max possible id. However, this isn't memory-efficient.
        // Should actually be the total number of unique nodes (in + out)
        val r = generateOffsetTablesForIn(inIteratorSeq, executorService, serializer, "stepi1r.txt", numShards, maxId max inMaxId, ren)
        idToIntOffsetIn = r._1; idToNumEdgesIn = r._2; edgeOffsetsIn = r._3

        // Step i2r - Allocate Shards
        log.info("Allocating in-shards (renumbered)...")
        allocateShards(serializer, "stepi2r.txt", shardDirectoriesIn, numShards, edgeOffsetsIn)

        // Step i3r - Write shards
        log.info("Writing to in-shards in rounds (renumbered)...")
        writeShardsInRoundsRenumbered(inIteratorSeq, executorService, serializer, "stepi3r.txt", shardDirectoriesIn,
          numShards, numRounds, edgeOffsetsIn, idToIntOffsetIn, idToNumEdgesIn, ren)
      }

    }
    else {
      log.info("Not renumbering...")

      // Step 2
      // Generate the lookup table for nodeIdSet
      // Generate and store shard offsets and # of edges for each node
      log.info("Generating offset tables...")
      serializer.writeOrRead("step2x.txt", { writer =>
        nodeIdSet = new mutable.BitSet(maxId+1)
        idToIntOffset = new Array[Long](maxId+1)
        idToNumEdges = new Array[Int](maxId+1)
        edgeOffsets = new Array[AtomicLong](numShards)
        (0 until numShards).foreach { i => edgeOffsets(i) = new AtomicLong() }
        Stats.time("graph_load_generating_offset_tables") {
          def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) {
            iteratorFunc() foreach { item =>
              val id = item.id
              val itemEdges = item.edges.length
              // Update nodeId set
              nodeIdSet(id) = true
              item.edges foreach { edge => nodeIdSet(edge) = true }
              // Store offsets
              val edgeOffset = edgeOffsets(id % numShards).getAndAdd(itemEdges)
              idToNumEdges(id) = itemEdges
              idToIntOffset(id) = edgeOffset
            }
          }
          ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
            outIteratorSeq, readOutEdges).toArray.map { future => future.asInstanceOf[Future[Unit]].get }
        }
        log.info("Writing offset tables to file...")
        writer.bitSet(nodeIdSet)
          .arrayOfLong(idToIntOffset)
          .arrayOfInt(idToNumEdges)
          .atomicLongArray(edgeOffsets).close
      }, { reader =>
        nodeIdSet = reader.bitSet()
        idToIntOffset = reader.arrayOfLong()
        idToNumEdges = reader.arrayOfInt()
        edgeOffsets = reader.atomicLongArray()
        reader.close
      })

      // Step 3 - Allocating shards
      log.info("Allocating shards...")
      allocateShards(serializer, "step3.txt", shardDirectories, numShards, edgeOffsets)

      // Step 4x - Generate shards on disk in rounds
      log.info("Writing to shards in rounds...")
      writeShardsInRounds(outIteratorSeq, executorService, serializer, "step4.txt", shardDirectories,
        numShards, numRounds, edgeOffsets, idToIntOffset, idToNumEdges)

      // Step 5 - Count number of nodes
      log.info("Counting total number of nodes...")
      serializer.writeOrRead("step5.txt", { writer =>
        Stats.time("graph_load_count_total_num_of_nodes") {
          for ( id <- 0 to maxId )
            if (nodeIdSet(id))
              numNodes += 1
          writer.integers(Seq(numNodes)).close
        }
      }, { reader =>
        numNodes = reader.int
        reader.close
      })

      if (inIteratorSeq != null) {
        // Step i1r - Generate Offset Table Counts
        log.info("Generating offset tables for in-shards...")
        val r = generateOffsetTablesForIn(inIteratorSeq, executorService, serializer, "stepi1.txt", numShards, inMaxId)
        idToIntOffsetIn = r._1; idToNumEdgesIn = r._2; edgeOffsetsIn = r._3

        // Step i2 - Allocate Shards
        log.info("Allocating in-shards...")
        allocateShards(serializer, "stepi2.txt", shardDirectoriesIn, numShards, edgeOffsetsIn)

        // Step i3 - Write shards
        log.info("Writing to in-shards in rounds...")
        writeShardsInRounds(inIteratorSeq, executorService, serializer, "stepi3.txt", shardDirectoriesIn,
          numShards, numRounds, edgeOffsetsIn, idToIntOffsetIn, idToNumEdgesIn)
      }
    }

    log.info("---Ended Load!---")

    // Speeds up existence checking especially if we renumber
    val nodeIdSetFn = if (renumber) {
      (i: Int) => { i > 0 && i <= numNodes }
    } else {
      (i: Int) => { nodeIdSet(i) }
    }

    // Used for bounds checking when getting a node
    val realMaxId = if (renumber) numNodes else maxId

    // Also the size of the idToIndex array allocated if we use FastLRU/FastClock
    val realMaxIdOutEdges = if (renumber) nodeWithOutEdgesCount else nodeWithOutEdgesMaxId
    val realMaxIdInEdges = realMaxId

    log.info("nodeWithOutEdgesCount is %s, nodeWithOutEdgesMaxId is %s".format(nodeWithOutEdgesCount,
      nodeWithOutEdgesMaxId))
    log.info("numNodes is %s, (original) maxId is %s".format(numNodes, maxId))
    log.info("realMaxId is %s, realMaxIdOutEdges is %s".format(realMaxId, realMaxIdOutEdges))

    // Parse out cacheType and nodeType for FastCachedDirectedGraph
    val (cType, nType) = if (cacheType.contains("_")) {
      val a = cacheType.split("_")
      (a(0), a(1))
    } else {
      (cacheType, "node")
    }

    // Return our graph!
    if (inIteratorSeq == null) { // Only one type of edge
      cacheType match {
        case "guava" => new GuavaCachedDirectedGraph(nodeIdSetFn, cacheMaxNodes+cacheMaxEdges,
          shardDirectories, numShards, idToIntOffset, idToNumEdges,
          maxId, realMaxId, realMaxIdOutEdges, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
          inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
          numNodes, numEdges, storedGraphDir)
        case "guava_na" => new NodeArrayGuavaCachedDirectedGraph(nodeIdSetFn, cacheMaxNodes+cacheMaxEdges,
          shardDirectories, numShards, idToIntOffset, idToNumEdges,
          maxId, realMaxId, realMaxIdOutEdges, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
          inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
          numNodes, numEdges, storedGraphDir)
        case _ => FastCachedDirectedGraph(nodeIdSetFn, cacheMaxNodes, cacheMaxEdges,
          shardDirectories, numShards, idToIntOffset, idToNumEdges,
          maxId, realMaxId, realMaxIdOutEdges, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
          inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
          numNodes, numEdges, storedGraphDir, cType, nType)
      }
    } else { // Both types of edges
      FastDualCachedDirectedGraph(nodeIdSetFn, cacheMaxNodes, cacheMaxEdges,
        shardDirectories, shardDirectoriesIn, numShards, idToIntOffset, idToNumEdges, idToIntOffsetIn, idToNumEdgesIn,
        maxId, realMaxId, realMaxIdOutEdges, realMaxIdInEdges, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
        inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
        numNodes, numEdges, storedGraphDir, cType, nType)
    }
  }

  @VisibleForTesting
  def apply(iteratorFunc: () => Iterator[NodeIdEdgesMaxId],
      storedGraphDir: StoredGraphDir, shardDirectory:String,
      cacheType:String, cacheDirectory:String = null,
      renumber: Boolean):CachedDirectedGraph =
    apply(Seq(iteratorFunc), null, MoreExecutors.sameThreadExecutor(),
      storedGraphDir, cacheType, 2, 4, Array(shardDirectory), null, 8, 2,
      useCachedValues = true, cacheDirectory = cacheDirectory, renumber = renumber)
}

abstract class CachedDirectedGraph(maxId: Int, realMaxId: Int,
    val nodeWithOutEdgesMaxId:Int, val nodeWithOutEdgesCount:Int, val inMaxId: Int,
    val nodeWithInEdgesMaxId: Int, val nodeWithInEdgesCount: Int) extends DirectedGraph {

  override lazy val maxNodeId = maxId

  def iterator = (0 to realMaxId).view.flatMap(getNodeById(_)).iterator

  def getMisses: Long

  def statsString: String

  def writeStats(fileName: String) {
    FileUtils.printToFile(new File(fileName))(p => {
      p.println(statsString)
    })
  }

  def getThreadSafeChild: CachedDirectedGraph
}

object FastCachedDirectedGraph {
  /**
   * An implementation of the directed graph trait backed by a cache of int arrays (and shards on the disk).
   * - If the nodeType is "node",
   *   - Assigns a couple of nodes per thread, and reuses the nodes each time getNodeById is called.
   *   -  Uses much less memory than NodeArray, but users must be careful to ALWAYS call getNodeById
   *      when accessing properties of a node within a single thread.
   *   - getNodeById always returns the same node object (simply mutating it before doing so).
   *   - In other words, do not save references to node objects.
   * - If the nodeType is "array",
   *   - Each thread gets allocated its own array of node objects.
   *   - Requires significantly more memory than using "node",
   *     but users can save references to node objects without fear.
   *
   * @param nodeIdSet nodes with either outgoing or incoming edges
   * @param cacheMaxNodes maximum number of nodes that the cache can store
   * @param cacheMaxEdges maximum number of edges that the cache can store
   * @param shardDirectories where shards live on disk
   * @param numShards number of shards to split into
   * @param idToIntOffset offset into a shard on disk
   * @param idToNumEdges the number of edges
   * @param maxId max node id in the graph
   * @param realMaxId the renumbered maxId (same as maxId if we didn't renumber)
   * @param realMaxIdOutEdges the renumbered maxId among Out Edges (only different if we renumbered)
   * @param nodeCount number of nodes in the graph
   * @param edgeCount number of edges in the graph
   * @param storedGraphDir the graph directions stored
   * @param cacheType the type of cache (clock, lru, random, etc.)
   * @param nodeType how nodes are returned (node, array, etc.)
   */
  def apply(nodeIdSet:(Int => Boolean),
    cacheMaxNodes:Int, cacheMaxEdges:Long,
    shardDirectories:Array[String], numShards:Int,
    idToIntOffset:Array[Long], idToNumEdges:Array[Int],
    maxId: Int, realMaxId: Int, realMaxIdOutEdges: Int, nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
    inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
    nodeCount: Int, edgeCount: Long, storedGraphDir: StoredGraphDir,
    cacheType: String = "lru", nodeType: String = "node"): CachedDirectedGraph = {

    val cache = cacheType match {
      case "lru" => FastLRUIntArrayCache(shardDirectories, numShards,
        realMaxIdOutEdges, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "bufflru" => BufferedFastLRUIntArrayCache(shardDirectories, numShards,
        realMaxIdOutEdges, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "lockfreereadlru" => LocklessReadFastLRUIntArrayCache(shardDirectories, numShards,
        realMaxIdOutEdges, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "random" => RandomizedIntArrayCache(shardDirectories, numShards,
        realMaxIdOutEdges, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "locklessrandom" => LocklessRandomizedIntArrayCache(shardDirectories, numShards,
        realMaxIdOutEdges, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "clock" => FastClockIntArrayCache(shardDirectories, numShards,
        realMaxIdOutEdges, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case _ => throw new IllegalArgumentException("Unknown cacheType %s".format(nodeType))
    }

    nodeType match {
      case "node" => new FastCachedDirectedGraph (nodeIdSet,
        cacheMaxNodes, cacheMaxEdges,
        shardDirectories, numShards,
        idToIntOffset, idToNumEdges,
        maxId, realMaxId, realMaxIdOutEdges, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
        inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
        nodeCount, edgeCount, storedGraphDir, cache)
      case "array" => new NodeArrayFastCachedDirectedGraph (nodeIdSet,
        cacheMaxNodes, cacheMaxEdges,
        shardDirectories, numShards,
        idToIntOffset, idToNumEdges,
        maxId, realMaxId, realMaxIdOutEdges, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
        inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
        nodeCount, edgeCount, storedGraphDir, cache)
      case _ => throw new IllegalArgumentException("Unknown nodeType %s".format(nodeType))
    }
  }
}

class FastCachedDirectedGraph (
    val nodeIdSet:(Int => Boolean),
    val cacheMaxNodes:Int, val cacheMaxEdges:Long,
    val shardDirectories:Array[String], val numShards:Int,
    val idToIntOffset:Array[Long], val idToNumEdges:Array[Int],
    maxId: Int, realMaxId: Int, realMaxIdOutEdges: Int, nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
    inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
    val nodeCount: Int, val edgeCount: Long, val storedGraphDir: StoredGraphDir, val cache: IntArrayCache)
    extends CachedDirectedGraph(maxId, realMaxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
      inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount) {

  def getThreadSafeChild = new FastCachedDirectedGraph(nodeIdSet,
    cacheMaxNodes, cacheMaxEdges,
    shardDirectories, numShards,
    idToIntOffset, idToNumEdges,
    maxId, realMaxId, realMaxIdOutEdges, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
    inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
    nodeCount, edgeCount, storedGraphDir, cache.getThreadSafeChild)

  def getMisses = cache.getStats._1

  def statsString: String = {
    val (misses, hits, currentSize, currRealCapacity) = cache.getStats
    "%s\t%s\t%s\t%s\t%s".format(misses, hits + misses, misses.toDouble / (hits+misses), currentSize, currRealCapacity)
  }

  val node = CachedDirectedNode(0, 0, storedGraphDir, cache)
  val someNode = Some(node)
  val emptyNode = EmptyDirectedNode(0, storedGraphDir)
  val someEmptyNode = Some(emptyNode)

  def getNodeById(id: Int) = { // Stats.time("cached_get_node_by_id")
    if (id > realMaxId || !nodeIdSet(id)) { // Invalid id
      None
    }
    else {
      val numEdges = try { idToNumEdges(id) } catch { case _ => 0 }
      if (numEdges == 0) {
        emptyNode.id = id
        someEmptyNode
      }
      else {
        node.id = id
        node.size = numEdges
        someNode
      }
    }
  }
}

class NodeArrayFastCachedDirectedGraph (
    val nodeIdSet:(Int => Boolean),
    val cacheMaxNodes:Int, val cacheMaxEdges:Long,
    val shardDirectories:Array[String], val numShards:Int,
    val idToIntOffset:Array[Long], val idToNumEdges:Array[Int],
    maxId: Int, realMaxId: Int, realMaxIdOutEdges: Int, nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
    inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
    val nodeCount: Int, val edgeCount: Long, val storedGraphDir: StoredGraphDir,
    val cache: IntArrayCache)
  extends CachedDirectedGraph(maxId, realMaxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
    inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount) {

  def getThreadSafeChild = new NodeArrayFastCachedDirectedGraph(nodeIdSet,
    cacheMaxNodes, cacheMaxEdges,
    shardDirectories, numShards,
    idToIntOffset, idToNumEdges,
    maxId, realMaxId, realMaxIdOutEdges, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
    inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
    nodeCount, edgeCount, storedGraphDir, cache.getThreadSafeChild)

  def getMisses = cache.getStats._1

  def statsString: String = {
    val (misses, hits, currentSize, currRealCapacity) = cache.getStats
    "%s\t%s\t%s\t%s\t%s".format(misses, hits + misses, misses.toDouble / (hits+misses), currentSize, currRealCapacity)
  }

  val emptyArray = new Array[Int](0)
  val nodeList = new Array[Option[Node]](maxId+1)

  def getNodeById(id: Int) = { // Stats.time("cached_get_node_by_id")
    if (id > realMaxId || !nodeIdSet(id)) // Invalid id
      None
    else {
      nodeList(id) match {
        case null => {
          val numEdges = try { idToNumEdges(id) } catch { case _ => 0 }
          if (numEdges == 0) {
            val node = Some(ArrayBasedDirectedNode(id, emptyArray, storedGraphDir))
            nodeList(id) = node
            node
          }
          else {
            val node = Some(CachedDirectedNode(id, numEdges, storedGraphDir, cache))
            nodeList(id) = node
            node
          }
        }
        case n => n
      }
    }
  }

}

/**
 * This is an implementation of the directed graph trait backed by a Google Guava cache
 * and shards on disk
 * Assigns a couple of nodes per thread, and reuses the nodes each time getNodeById
 * is called.
 * Uses much less memory than NodeArray, but may introduce concurrency issues!
 * @param nodeIdSet nodes with either outgoing or incoming edges
 * @param cacheMaxNodesAndEdges maximum number of nodes and edges that the cache can store
 * @param shardDirectories where shards live on disk
 * @param numShards number of shards to split into
 * @param idToIntOffset offset into a shard on disk
 * @param idToNumEdges offset into a shard on disk and the number of edges
 * @param maxId max node id in the graph
 * @param realMaxId the actual maxId (only different if we renumbered)
 * @param realMaxIdOutEdges the actual maxId among Out Edges (only different if we renumbered)
 * @param nodeCount number of nodes in the graph
 * @param edgeCount number of edges in the graph
 * @param storedGraphDir the graph directions stored
 */
class GuavaCachedDirectedGraph (
    val nodeIdSet:(Int => Boolean),
    val cacheMaxNodesAndEdges: Long,
    val shardDirectories:Array[String], val numShards:Int,
    val idToIntOffset:Array[Long], val idToNumEdges:Array[Int],
    maxId: Int, realMaxId: Int, realMaxIdOutEdges: Int, nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
    inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
    val nodeCount: Int, val edgeCount: Long, val storedGraphDir: StoredGraphDir)
    extends CachedDirectedGraph(maxId, realMaxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
      inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount) {

  val reader = new MultiDirIntShardsReader(shardDirectories, numShards)

  def getThreadSafeChild = throw new Exception("No multithreaded Guava Cache for now")

  // Array Loader
  private def loadArray(id: Int):Array[Int] = {
    val numEdges = try { idToNumEdges(id) } catch { case _ => 0 }
    if (numEdges == 0) {
      throw new NullPointerException("Guava loadArray idToIntOffsetAndNumEdges %s".format(id))
    }
    else {
      // Read in the node from disk
      val intArray = new Array[Int](numEdges)
      reader.readIntegersFromOffsetIntoArray(id, idToIntOffset(id), numEdges, intArray, 0)
      intArray
    }
  }

  // Guava Cache
  val cacheG:LoadingCache[Int,Array[Int]] = CacheBuilder.newBuilder()
    .maximumWeight(cacheMaxNodesAndEdges)
    .weigher(new Weigher[Int,Array[Int]] {
      def weigh(k:Int, v:Array[Int]):Int = v.length
    })
    .asInstanceOf[CacheBuilder[Int,Array[Int]]]
    .build[Int,Array[Int]](new CacheLoader[Int,Array[Int]] {
      def load(k:Int):Array[Int] = loadArray(k)
    })

  val cache = new IntArrayCache {
    def get(id: Int) = cacheG.get(id)
    def getStats = throw new IllegalArgumentException("Can't call getStats on this!")
    def getThreadSafeChild = this
  }

  def getMisses = cacheG.stats().missCount()

  def statsString: String = {
    val stats = cacheG.stats()
    "%s\t%s\t%s\t%s".format(stats.missCount(), stats.requestCount(), stats.missRate(), stats.averageLoadPenalty())
  }

  val nodeMap = new mutable.HashMap[Long,Option[CachedDirectedNode]]()
  val emptyNodeMap = new mutable.HashMap[Long,Option[CachedDirectedNode]]()

  def getNodeById(id: Int) = { // Stats.time("cached_get_node_by_id")
    if (id > realMaxId || !nodeIdSet(id)) { // Invalid id
      None
    }
    else {
      val numEdges = try { idToNumEdges(id) } catch { case _ => 0 }
      try {
        if (numEdges == 0) {
          val node = emptyNodeMap(Thread.currentThread().getId)
          node.get.id = id
          node
        }
        else {
          val node = nodeMap(Thread.currentThread().getId)
          node.get.id = id
          node.get.size = numEdges
          node
        }
      } catch {
        case e: NoSuchElementException => {
          val node = Some(CachedDirectedNode(id, numEdges, storedGraphDir, cache))
          val emptyNode = Some(EmptyDirectedNode(id, storedGraphDir))
          nodeMap.put(Thread.currentThread().getId, node)
          emptyNodeMap.put(Thread.currentThread().getId, emptyNode)
          if (numEdges == 0)
            emptyNode
          else
            node
        }
      }
    }
  }

}

/**
 * This is an implementation of the directed graph trait backed by a Google Guava cache
 * and shards on disk
 * Initializes an empty array, and fills it with node objects
 * Consumes significantly more memory than ReusableNode, although it does not suffer from
 * concurrency issues
 * @param nodeIdSet nodes with either outgoing or incoming edges
 * @param cacheMaxNodesAndEdges maximum number of nodes and edges that the cache can store
 * @param shardDirectories where shards live on disk
 * @param numShards number of shards to split into
 * @param idToIntOffset offset into a shard on disk
 * @param idToNumEdges offset into a shard on disk and the number of edges
 * @param maxId max node id in the graph
 * @param nodeCount number of nodes in the graph
 * @param edgeCount number of edges in the graph
 * @param storedGraphDir the graph directions stored
 */
class NodeArrayGuavaCachedDirectedGraph (
                                 val nodeIdSet:(Int => Boolean),
                                 val cacheMaxNodesAndEdges: Long,
                                 val shardDirectories:Array[String], val numShards:Int,
                                 val idToIntOffset:Array[Long], val idToNumEdges:Array[Int],
                                 maxId: Int, realMaxId: Int, realMaxIdOutEdges: Int, nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
                                 inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
                                 val nodeCount: Int, val edgeCount: Long, val storedGraphDir: StoredGraphDir)
  extends CachedDirectedGraph(maxId, realMaxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
    inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount) {

  val reader = new MultiDirIntShardsReader(shardDirectories, numShards)

  def getThreadSafeChild = throw new Exception("No multithreaded Guava Cache for now")

  // Array Loader
  private def loadArray(id: Int):Array[Int] = {
    val numEdges = try { idToNumEdges(id) } catch { case _ => 0 }
    if (numEdges == 0) {
      throw new NullPointerException("Guava loadArray idToIntOffsetAndNumEdges %s".format(id))
    }
    else {
      // Read in the node from disk
      val intArray = new Array[Int](numEdges)
      reader.readIntegersFromOffsetIntoArray(id, idToIntOffset(id), numEdges, intArray, 0)
      intArray
    }
  }

  // Guava Cache
  val cacheG:LoadingCache[Int,Array[Int]] = CacheBuilder.newBuilder()
    .maximumWeight(cacheMaxNodesAndEdges)
    .weigher(new Weigher[Int,Array[Int]] {
    def weigh(k:Int, v:Array[Int]):Int = v.length
  })
    .asInstanceOf[CacheBuilder[Int,Array[Int]]]
    .build[Int,Array[Int]](new CacheLoader[Int,Array[Int]] {
    def load(k:Int):Array[Int] = loadArray(k)
  })

  val cache = new IntArrayCache {
    def get(id: Int) = cacheG.get(id)
    def getStats = throw new IllegalArgumentException("Can't call getStats on this!")
    def getThreadSafeChild = this
  }

  def getMisses = cacheG.stats().missCount()

  def statsString: String = {
    val stats = cacheG.stats()
    "%s\t%s\t%s\t%s".format(stats.missCount(), stats.requestCount(), stats.missRate(), stats.averageLoadPenalty())
  }

  val emptyArray = new Array[Int](0)
  val nodeList = new Array[Option[Node]](maxId+1)

  def getNodeById(id: Int) = { // Stats.time("cached_get_node_by_id")
    if (id > realMaxId || !nodeIdSet(id)) // Invalid id
      None
    else
      nodeList(id) match {
        case null => {
          val numEdges = try { idToNumEdges(id) } catch { case _ => 0 }
          if (numEdges == 0) {
            val node = Some(ArrayBasedDirectedNode(id, emptyArray, storedGraphDir))
            nodeList(id) = node
            node
          }
          else {
            val node = Some(CachedDirectedNode(id, numEdges, storedGraphDir, cache))
            nodeList(id) = node
            node
          }
        }
        case n => n
      }
  }
}