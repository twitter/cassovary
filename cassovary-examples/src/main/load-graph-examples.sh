#!/bin/bash

echo 'Building the jars'
./sbt ';project cassovary-examples ;assembly ;assemblyPackageDependency ;assemblyPackageScala'
JARDIR=cassovary-examples/target/scala-2.11
JARS=`echo $JARDIR/*assembly*.jar`
JARSCP=${JARS// /:}

echo Generating a random graph with 5M nodes with average outdegree 20 and storing in /tmp/...
java -Xmx1500M -server -cp $JARSCP WriteRandomGraph /tmp 5M_20 5000000 20 10

echo 'Loading graph from one of these files (approx. 0.5M nodes and 10M edges) in out direction'
java -Xmx100M -server -cp $JARSCP HelloLoadSharedGraph /tmp 5M_20_0.txt OnlyOut

echo 'Loading graph from all the files (approx. 5M nodes and 100M edges) in out direction'
java -Xmx500M -server -cp $JARSCP HelloLoadSharedGraph /tmp 5M_20 OnlyOut

echo 'Loading graph from one of these files (approx. 0.5M nodes and 10M edges) in both directions'
java -Xmx500M -server -cp $JARSCP HelloLoadSharedGraph /tmp 5M_20_0.txt BothInOut

echo 'Loading graph from all the files (approx. 5M nodes and 100M edges) in both directions'
java -Xmx1300M -server -cp $JARSCP HelloLoadSharedGraph /tmp 5M_20 BothInOut

