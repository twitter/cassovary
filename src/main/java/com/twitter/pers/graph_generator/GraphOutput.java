package com.twitter.pers.graph_generator;

/**
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, akyrola@twitter.com
 */
public interface GraphOutput {

    void addEdges(int[] from, int[] to);

    void finishUp();

}
