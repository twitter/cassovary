package com.twitter.pers.graph_generator;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Efficient parallel output of edges into a binary edge list format.
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, akyrola@twitter.com
 */
public class EdgeListOutput implements GraphOutput {

    private String fileNamePrefix;

    static int partSeq = 0;

    public EdgeListOutput(String fileNamePrefix) {
        this.fileNamePrefix = fileNamePrefix;
    }

    @Override
    public void addEdges(int[] from, int[] to)  {
        try {
            DataOutputStream dos = partitionOut.get();
            int n = from.length;
            for(int i=0; i<n; i++) {
                dos.writeInt(Integer.reverseBytes(from[i]));
                dos.writeInt(Integer.reverseBytes(to[i]));
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public void finishUp() {
        try {
            partitionOut.get().close();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    /* Each thread will have a local partition */
    private ThreadLocal<DataOutputStream> partitionOut = new ThreadLocal<DataOutputStream>() {
        @Override
        protected DataOutputStream initialValue() {
            try {
                int thisPartId;
                synchronized (this) {
                    thisPartId = partSeq++;
                }

                String fileName = fileNamePrefix + "-part" + thisPartId;
                return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
            } catch (Exception err) {
                err.printStackTrace();
                throw new RuntimeException(err);
            }
        }
    };

}
