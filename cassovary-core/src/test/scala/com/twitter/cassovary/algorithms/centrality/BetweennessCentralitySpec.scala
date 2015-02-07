package com.twitter.cassovary.algorithms.centrality

import com.twitter.cassovary.graph.GraphDir._
import com.twitter.cassovary.graph._
import com.twitter.cassovary.graph.tourist.{PrevNbrCounter, PathsCounter}
import com.twitter.cassovary.util.FastUtilUtils
import com.twitter.finagle.stats.DefaultStatsReceiver
import it.unimi.dsi.fastutil.ints.Int2IntMap
import it.unimi.dsi.fastutil.objects.Object2IntMap
import org.scalatest.{Matchers, WordSpec}

/**
 * Created by bmckown on 2/6/15
 */
class BetweennessCentralitySpec extends WordSpec with Matchers {
  lazy val graph = TestGraphs.g6
  "Betweenness centrality" should {
    "do some cool shit" in {
      val visitor = new PathsCounter(10, Seq(10))
      val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(10), Walk.Limits())

      graph foreach { n =>
        visitor.visit(n.id)
      }
      val info = visitor.infoAllNodes

      def pathMapToSeq(map: Object2IntMap[DirectedPath]) = {
        FastUtilUtils.object2IntMapToArray(map).toSeq
      }

      pathMapToSeq(info(15)) should be (Seq(DirectedPath(Array(11))))
    }
  }
}
