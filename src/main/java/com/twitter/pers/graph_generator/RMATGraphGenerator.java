package com.twitter.pers.graph_generator;

import java.util.ArrayList;
import java.util.Random;

/**
 * Graph generator based on the R-MAT algorithm
 * R-MAT: A Recursive Model for Graph Mining
 * Chakrabarti, Zhan, Faloutsos: http://www.cs.cmu.edu/~christos/PUBLICATIONS/siam04.pdf
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
        int nThreads = Runtime.getRuntime().availableProcessors();

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

        long t = System.currentTimeMillis();
        RMATGraphGenerator generator = new RMATGraphGenerator(new EdgeListOutput(outputFile),
                pA, pB, pC, pD, numVertices, numEdges);
        generator.execute();
        System.out.println("Generating took " + (System.currentTimeMillis() - t) * 0.001 + " secs");

    }

}
