package com.twitter.pers.graph_generator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Outputs edges into HDFS in tab delimited edge list format
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, akyrola@twitter.com
 */
public class HDFSEdgeListOutput implements GraphOutput {

    private String fileNamePrefix;

    static int partSeq = 0;

    public HDFSEdgeListOutput(String fileNamePrefix) {
        this.fileNamePrefix = fileNamePrefix;
        System.out.println("Using HDFS output: " + fileNamePrefix);
    }

    @Override
    public void addEdges(int[] from, int[] to)  {
        try {
            FSDataOutputStream dos = partitionOut.get();
            int n = from.length;
            StringBuffer sb = new StringBuffer(from.length * 32);
            for(int i=0; i<n; i++) {
                sb.append(from[i]);
                sb.append("\t");
                sb.append(to[i]);
                sb.append("\n");
            }
            dos.write(sb.toString().getBytes());
        } catch (Exception ioe) {
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
    private ThreadLocal<FSDataOutputStream> partitionOut = new ThreadLocal<FSDataOutputStream>() {
        @Override
        protected FSDataOutputStream initialValue() {
            try {
                int thisPartId;
                synchronized (this) {
                    thisPartId = partSeq++;
                }

                String hadoopHome = System.getProperty("HADOOP_HOME");
                if (hadoopHome == null) hadoopHome = System.getenv("HADOOP_HOME");

                if (hadoopHome == null) {
                    throw new IllegalArgumentException("You need to specify environment variable or JVM option HADOOP_HOME!");
                }

                Configuration conf = new Configuration();
                conf.addResource(new Path(hadoopHome + "/conf/core-site.xml"));
                conf.addResource(new Path(hadoopHome + "/conf/hdfs-site.xml"));


                String fileName = fileNamePrefix + "-part" + thisPartId;
                FileSystem fs = FileSystem.get(conf);
                return fs.create(new Path(fileName));
            } catch (Exception err) {
                err.printStackTrace();
                throw new RuntimeException(err);
            }
        }
    };

}
