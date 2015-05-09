#!/bin/bash

INPUT_GRAPH_FILES_PREFIX=$1
OUTPUT_GRAPH_DIR=$2

ALL_INSTANCES_SUBDIR=$OUTPUT_GRAPH_DIR/all_instances
OUTDEGREES=$ALL_INSTANCES_SUBDIR/outdegrees.txt
INDEGREES=$ALL_INSTANCES_SUBDIR/indegrees.txt

mkdir -p $ALL_INSTANCES_SUBDIR
echo Creating Outdegrees file in $OUTDEGREES ...
grep -h '. .' $INPUT_GRAPH_FILES_PREFIX* > $OUTDEGREES

echo Creating Indegrees file in $INDEGREES ...
grep -h '^[0-9][0-9]*$' $INPUT_GRAPH_FILES_PREFIX* | sort -S2G | uniq -c | perl -lane 'print $F[1]," ", $F[0]' > $INDEGREES
echo Done everything.

