
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


package com.twitter.pers.graph_generator;

import java.util.ArrayList;
import java.util.Random;

/**
 * Graph generator based on the R-MAT algorithm
 * R-MAT: A Recursive Model for Graph Mining
 * Chakrabarti, Zhan, Faloutsos: http://www.cs.cmu.edu/~christos/PUBLICATIONS/siam04.pdf
 *
 * Usage:
 *    [outputfile-prefix] [num-vertices] [num-edges] probA probB probC probD
 *
 * See the paper for the description of parameters probA, probB, probC, probD.
 *
 * If the outputfile-prefix starts with hdfs://, the graph is written into HDFS as tab-delimited
 * partitions.
 *
 * Specify the number of threads used by passing a JVM option -Dnum_threads
 * Each thread will create its own partition.
 *
 * Note that the result may (will) contain duplicate edges.
 *
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, akyrola@twitter.com
 */
public class RMATGraphGenerator {

    private GraphOutput outputter;

    /* Parameters for top-left, top-right, bottom-left, bottom-right probabilities */
    private double pA, pB, pC, pD;
    private long numEdges;
    private int numVertices;

    /**
     *  From http://pywebgraph.sourceforge.net
     ## Probability of choosing quadrant A
     self.probA = 0.45

     ## Probability of choosing quadrant B
     self.probB = 0.15

     ## Probability of choosing quadrant C
     self.probC = 0.15

     ## Probability of choosing quadrant D
     self.probD = 0.25
     */


    public RMATGraphGenerator(GraphOutput outputter, double pA, double pB, double pC, double pD, int nVertices,long nEdges) {
        this.outputter = outputter;
        this.pA = pA;
        this.pB = pB;
        this.pC = pC;
        this.pD = pD;

        if (Math.abs(pA + pB + pC + pD - 1.0) > 0.01)
            throw new IllegalArgumentException("Probabilities do not add up to one!");
        numVertices = nVertices;
        numEdges = nEdges;
    }

    public void execute() {
        int nThreads = Integer.parseInt(System.getProperty("num_threads", Runtime.getRuntime().availableProcessors() + ""));

        ArrayList<Thread> threads = new ArrayList<Thread>();
        for(int i=0; i < nThreads; i++) {
            Thread t = new Thread(new RMATGenerator(numEdges / nThreads));
            t.start();
            threads.add(t);
        }

        /* Wait */
        try {
            for(Thread t : threads) t.join();
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

    private class RMATGenerator implements Runnable {

        private long edgesToGenerate;

        private RMATGenerator(long genEdges) {
            this.edgesToGenerate = genEdges;
        }

        public void run() {
            int nEdgesATime = 1000000;
            long createdEdges = 0;

            Random r = new Random(System.currentTimeMillis() + this.hashCode());

            double cumA = pA;
            double cumB = cumA + pB;
            double cumC = cumB + pC;
            double cumD = 1.0;
            assert(cumD > cumC);

            while(edgesToGenerate > createdEdges) {
                int ne = (int) Math.min(edgesToGenerate  - createdEdges, nEdgesATime);
                int[] fromIds = new int[ne];
                int[] toIds = new int[ne];

                for(int j=0; j < ne; j++) {
                    int col_st = 0, col_en = numVertices - 1, row_st = 0, row_en = numVertices - 1;
                    while (col_st != col_en || row_st != row_en) {
                        double x = r.nextDouble();

                        if (x < cumA) {
                            // Top-left
                            col_en = col_st + (col_en - col_st) / 2;
                            row_en = row_st + (row_en - row_st) / 2;
                        } else if (x < cumB) {
                            // Top-right
                            col_st = col_en - (col_en - col_st) / 2;
                            row_en = row_st + (row_en - row_st) / 2;

                        } else if (x < cumC) {
                            // Bottom-left
                            col_en = col_st + (col_en - col_st) / 2;
                            row_st = row_en - (row_en - row_st) / 2;
                        } else {
                            // Bottom-right
                            col_st = col_en - (col_en - col_st) / 2;
                            row_st = row_en - (row_en - row_st) / 2;
                        }
                    }
                    fromIds[j] = col_st;
                    toIds[j] = row_st;
                }

                outputter.addEdges(fromIds,  toIds);
                createdEdges += ne;
                System.out.println(Thread.currentThread().getId() + " created " + createdEdges + " edges.");
            }
            outputter.finishUp();
        }
    }

    public static void main(String[] args) {
        int k = 0;
        String outputFile = args[k++];
        int numVertices = Integer.parseInt(args[k++]);
        long numEdges = Long.parseLong(args[k++]);

        double pA = Double.parseDouble(args[k++]);
        double pB = Double.parseDouble(args[k++]);
        double pC = Double.parseDouble(args[k++]);
        double pD = Double.parseDouble(args[k++]);

        System.out.println("Going to create graph with approx. " + numVertices + " vertices and " + numEdges + " edges");

        GraphOutput outputInstance = null;
        if (outputFile.startsWith("hdfs://")) outputInstance = new HDFSEdgeListOutput(outputFile);
        else outputInstance = new EdgeListOutput(outputFile);

        long t = System.currentTimeMillis();
        RMATGraphGenerator generator = new RMATGraphGenerator(outputInstance,
                pA, pB, pC, pD, numVertices, numEdges);
        generator.execute();
        System.out.println("Generating took " + (System.currentTimeMillis() - t) * 0.001 + " secs");

    }

}
