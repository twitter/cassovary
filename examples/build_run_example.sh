#!/bin/bash

BUILD_PACKAGE="build"

COMPILER="$1"
SUBDIR="$2"
EXAMPLE="$3"
NUM_NODES=$4


root=$(
  cd $(dirname $0)/..
  echo $PWD
)
echo root is $root
cd $root

if [ $BUILD_PACKAGE = build ]; then
  echo Downloading dependent jars...
  $root/sbt update
  SBT_UPDATE_RET=$?
  if [ $SBT_UPDATE_RET -ne 0 ]; then
     echo "Error: Downloading dependent jars failed with exit code $SBT_UPDATE_RET"
     exit $SBT_UPDATE_RET
  fi
  echo Building Cassovary jar...
  $root/sbt package
  SBT_PACKAGE_RET=$?
  if [ $SBT_PACKAGE_RET -ne 0 ]; then
    echo "Error: Building Cassovary jar failed with exit code $SBT_PACKAGE_RET"
    exit $SBT_PACKAGE_RET
  fi
fi

SCALA_LIB_JAR=$(find $root/lib_managed -name 'scala-library*.jar' | head -1)
echo "Using scala library $SCALA_LIB_JAR"

JAVA_CP=(
  $(find $root/target -name 'cassovary*.jar') \
  $SCALA_LIB_JAR \
  $(find $root/lib_managed/jars/ -name '*.jar')
)
JAVA_CP=$(echo ${JAVA_CP[@]} | tr ' ' ':')

cd $SUBDIR
mkdir -p classes
rm -rf classes/*
echo Compiling $EXAMPLE ...
$COMPILER -cp $JAVA_CP -d classes $EXAMPLE
SCALAC_RET=$?
if [ $SCALAC_RET -ne 0 ]; then
  echo "Error: scalac failed with exit code $SCALAC_RET"
  exit $SCALAC_RET
fi

echo Running $EXAMPLE...
JAVA_OPTS="-server -Xmx1g -Xms1g"
java ${JAVA_OPTS} -cp $JAVA_CP:classes ${EXAMPLE%.*} $NUM_NODES
