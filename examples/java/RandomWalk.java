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

/**
 * The Java version of the RandomWalk example.
 */

import com.google.common.base.Functions;
import com.google.common.collect.Ordering;
import com.twitter.cassovary.graph.DirectedGraph;
import com.twitter.cassovary.graph.DirectedPath;
import com.twitter.cassovary.graph.GraphDir;
import com.twitter.cassovary.graph.GraphUtils;
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams;
import com.twitter.cassovary.graph.TestGraphs;
import com.twitter.util.Duration;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import java.lang.Integer;
import java.lang.System;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import scala.Option;
import scala.Tuple2;

public class RandomWalk {

  public static void main(String[] args) {
    int numNodes = 3;
    if (args.length > 0) numNodes = Integer.parseInt(args[0]);
    System.out.printf("Generating a random graph with %d nodes...\n", numNodes);
    DirectedGraph graph = TestGraphs.generateRandomGraph(numNodes, Math.min(10, numNodes));
    System.out.printf("Generated a random directed graph with %s nodes and %s edges.\n", graph.nodeCount(),
            graph.edgeCount());

    // Generate walk parameters
    long numSteps = 1000 * 1000;
    final scala.Option<Integer> wpNone = scala.Option.apply(null);
    final scala.Option<Integer> wpTwo = scala.Option.apply(2);
    RandomWalkParams walkParams = new RandomWalkParams(numSteps, 0.1, wpNone, wpTwo, wpNone, false, GraphDir.OutDir(),
            false, false);
    GraphUtils graphUtils = new GraphUtils(graph);

    // Do the walk and measure how long it took
    System.out.printf("Now doing a random walk of %s steps from Node 0...\n", numSteps);
    long startTime = System.nanoTime();
    Tuple2<Int2IntMap, scala.Option<Int2ObjectMap<Object2IntMap<DirectedPath>>>> lm =
            graphUtils.calculatePersonalizedReputation(0, walkParams);
    long endTime = System.nanoTime();
    Int2IntMap neighbors = lm._1;
    System.out.printf("Random walk visited %s nodes in %s ms:\n", neighbors.size(), (endTime - startTime)/1000000);

    // Sort neighbors (or nodes) in descending number of visits and take the top 10 neighbors
    List<Integer> topNeighbors = Ordering.natural().onResultOf(Functions.forMap(neighbors)).reverse()
            .immutableSortedCopy(neighbors.keySet());
    
    if (topNeighbors.size() > 10) topNeighbors = topNeighbors.subList(0, 10);

    // Print the top 10 neighbors (and paths)
    System.out.printf("%8s%10s\t%s\n", "NodeID", "#Visits", "Top 2 Paths with counts");
    for (int id : topNeighbors) {
      int numVisits = neighbors.get(id);
      System.out.printf("%8s%10s\t", id, numVisits);
      if (lm._2.isDefined()) { // If Option is not None
        Object2IntMap<DirectedPath> paths = lm._2.get().get(id);
        int remaining = paths.size();
        for (Map.Entry<DirectedPath, Integer> ef : paths.entrySet()) {
          // Print a directed path and #visits along that path
          int[] nodes = ef.getKey().nodes();
          for (int i = 0; i < nodes.length; i++) {
            if (i != 0) System.out.printf("->%d", nodes[i]);
            else System.out.printf("%d", nodes[i]);
          }
          System.out.printf(" (%d)", ef.getValue());
          if (remaining > 1) System.out.printf(" | ");
          remaining--;
        }
      }
      System.out.println();
    }
  }
}