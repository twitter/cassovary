package com.twitter.cassovary.algorithms


import com.twitter.cassovary.util.io.AdjacencyListGraphReader
import java.util.concurrent.Executors
import util.Sorting
import scala.collection.immutable.TreeMap


object PagerankRunner {


  case class RankValue(value : Double, id : Int)

  object ReverseOrdering extends  Ordering[RankValue] {
    def compare(cur: RankValue, that: RankValue) = {
      if (cur.value == that.value) cur.id compareTo that.id else that.value compareTo cur.value } }

  def main(args: Array[String]) {
    val graph = new AdjacencyListGraphReader("/Users/akyrola/graphs/",
      "toy_6nodes_adj")  {
      override val executorService = Executors.newFixedThreadPool(2)
    }.toArrayBasedDirectedGraph()

    printf("Graph has %s nodes, %s directed edges.\n",
      graph.nodeCount, graph.edgeCount)

    val params = PageRankParams(0.1, Some(5))
    val ranks = PageRank(graph, params)

    // Very ugly
    val topN = if (5 > graph.nodeCount) graph.nodeCount else 5
    var topList: TreeMap[ RankValue, Int ] = new TreeMap[ RankValue, Int ] () (ReverseOrdering)

    var intop = 0;
    var i = 0
    println(topN)
    ranks.foreach(rv => {
       if (topList.size < topN) topList = topList.insert(RankValue(rv, i), i)
       else {
          if (topList.lastKey.value < rv)  topList = topList.insert(RankValue(rv, i), i)
       }
      i += 1
    })

    topList.keys.foreach( rv => printf("%d = %f\n", rv.id, rv.value))
  }

}
