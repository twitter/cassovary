#!/bin/bash

#Turn on if profiling via yourkit
#PROFILE_OPT="-agentpath:/Applications/mydownloads/YourKit_Java_Profiler_2015_build_15074.app/Contents/Resources/bin/mac/libyjpagent.jnilib"

echoandrun () {
    echo "###NEW RUN:###"
    echo "Loading graph from graph files with $2 nodes in direction $3"
    cmd="java -Xmx$1 -server -cp $JARSCP $PROFILE_OPT HelloLoadSharedGraph /tmp $2 $3"
    echo "Running: $cmd"
    eval $cmd
}

echo 'Building the jars'
./sbt ';project cassovary-examples ;assembly ;assemblyPackageDependency ;assemblyPackageScala'
JARDIR=cassovary-examples/target/scala-2.11
JARS=`echo $JARDIR/*assembly*.jar`
JARSCP=${JARS// /:}

#echo Generating a random graph with 5M nodes with average outdegree 20 (so ~100M edges total in one direction) and storing in /tmp/...
java -Xmx1500M -server -cp $JARSCP WriteRandomGraph /tmp 5M_20 5000000 20 8

#echo Generating a random graph with 500K nodes with average outdegree 20 (so ~10M edges total in one direction) and storing in /tmp/...
java -Xmx1500M -server -cp $JARSCP WriteRandomGraph /tmp 500K_20 500000 20 4

echoandrun 60M 500K OnlyOut
echoandrun 120M 500K BothInOut

echoandrun 500M 5M OnlyOut
echoandrun 1100M 5M BothInOut
