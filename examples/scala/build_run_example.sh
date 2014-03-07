#!/bin/bash

#PLEASE NOTE: the examples only work with scala 2.9.3 right now
#To make them work with scala 2.10, change the scala-library-2.9.3.jar
#in examples/build_run_example.sh to scala-library-2.10.*.jar
COMPILER="scalac"
#COMPILER="/usr/local/Cellar/scala/2.10.3/bin/scalac"
SUBDIR="examples/scala"
SCALA_VERSION=$($COMPILER -version 2>&1 | grep Scala | sed -e 's/Scala compiler version \(2.*\) --.*/\1/')

if [ -z "$1" ]; then
  EXAMPLE=HelloGraph.scala
else
  EXAMPLE="$1".scala
  shift
fi

if [ -z "$1" ]; then
  NUM_NODES=100
else
  NUM_NODES=$1
  shift
fi

$(dirname $0)/../build_run_example.sh $COMPILER $SCALA_VERSION $SUBDIR $EXAMPLE $NUM_NODES
