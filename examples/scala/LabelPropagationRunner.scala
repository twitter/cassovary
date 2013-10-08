/*
 * Copyright 2012 Twitter, Inc.
 * Author: David Fuhry
 *
 */
/**
 * Loads VIT / Topic graph from a file and shows stats.
 *
 * Specifically, it loads an ArrayBasedDirectedGraph in using AdjacencyListGraphReader,
 * and configures the reader to use 2 threads to load the graph instead of just one.
 * This example loads in both toy_6nodes_adj_1.txt and toy_6nodes_adj_2.txt from
 * src/test/resources/graphs/
 */

import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.graph.{DirectedPath, GraphUtils, TestGraphs}
import com.twitter.cassovary.util.io.AdjacencyListGraphReader
import com.twitter.cassovary.algorithms.LabelPropagation;
import com.twitter.cassovary.algorithms.LabelPropagationParams;
import com.twitter.util.Duration
import java.util.concurrent.Executors
import com.google.common.util.concurrent.MoreExecutors
import scala.collection.JavaConversions._
import scala.io.Source;

object LabelPropagationRunner {
  def topicIdxToInt(tidxStr: String): Int = {
    if (tidxStr == "") -1
    else tidxStr.toInt
  }
    

  def main(args: Array[String]) {
    val idxUidMap = Map(Source.fromFile("/Users/dfuhry/gssl/dfuhry-kfu-edges-7.uid_map").getLines().zipWithIndex.map{ case (s,i) => (i,s) }.toList: _ *)
    val idxTopicMap = Map(Source.fromFile("/Users/dfuhry/gssl/dfuhry-kfu-edges-7.topic1_t1id_map").getLines().zipWithIndex.map{ case (s,i) => (i,s) }.toList: _ *)

    var uidxTidx = (Source.fromFile("/Users/dfuhry/gssl/dfuhry-kfu-edges-7.uincrid_t1incrid").getLines() map ( tidxStr => topicIdxToInt(tidxStr) )).toArray
    //{ case ("") => -1 case (s) => s.toInt }.toList;

    val uidSNLines = Source.fromFile("/Users/dfuhry/gssl/dfuhry_kfu_users_ids").getLines()
    val uidSNSplit = uidSNLines.map(_.split("\t")).map{case Array(s1, s2) => Pair(s1, s2) case _ => Pair("", "")}
    val uidSNMap = Map(uidSNSplit.toList: _ *)

    val graph = new AdjacencyListGraphReader("/Users/dfuhry/gssl/", "dfuhry-kfu-edges-7_cassovary_adj_") {
      override val executorService = Executors.newFixedThreadPool(4)
      //override val executorService = Executors.newSingleThreadExecutor()
      //override val executorService = MoreExecutors.sameThreadExecutor()
    }.toArrayBasedDirectedGraph()

    printf("Loaded graph loaded has %s nodes and %s directed edges.\n",
      graph.nodeCount, graph.edgeCount)

    val params = LabelPropagationParams(0.15, Some(10))
    //val pr = PageRank(graph, params)
    val lp = LabelPropagation(graph, uidxTidx, params)

    /*
    lp.zipWithIndex.sortWith(_._1 < _._1).foreach { case(prVal, idx) =>  {
        val id = idxUidMap.get(idx).get;
        //printf("PageRank: %f\n", prVal);
        //printf("Id: %s\n", id);
        //printf("ScreenName: %s\n", uidSNMap.get(id).get);
        printf("%f\t%s\n", prVal, uidSNMap.get(id).get);
      }
    }
    */
    printf("label propagation result: %s\n", lp);

    printf("finished.\n")
  }
}
