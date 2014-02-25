#!/bin/bash

COMPILER="javac"
SUBDIR="examples/java"

if [ -z "$1" ]; then
  EXAMPLE=HelloGraph.java
else
  EXAMPLE="$1".java
  shift
fi

if [ -z "$1" ]; then
  NUM_NODES=100
else
  NUM_NODES=$1
  shift
fi

$(dirname $0)/../build_run_example.sh $COMPILER $SUBDIR $EXAMPLE $NUM_NODES
