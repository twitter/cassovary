#!/bin/bash

COMPILER="scalac"
SUBDIR="examples/scala"

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

$(dirname $0)/../build_run_example.sh $COMPILER $SUBDIR $EXAMPLE $NUM_NODES
