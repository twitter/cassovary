#!/bin/bash

BUILD_PACKAGE="build"

if [ -z "$1" ]; then
  EXAMPLE=HelloGraph
else
  EXAMPLE="$1"
  shift
fi

if [ -z "$1" ]; then
  NUM_NODES=100
else
  NUM_NODES=$1
  shift
fi


root=$(
  cd $(dirname $0)/../..
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

JAVA_CP=(
  $(find $root/target -name 'cassovary*.jar') \
  $(ls -t -1 $HOME/.sbt/boot/scala-*/lib/scala-library.jar | head -1) \
  $(find $root/lib_managed/jars/ -name '*.jar')
)
JAVA_CP=$(echo ${JAVA_CP[@]} | tr ' ' ':')

cd examples/java
mkdir -p classes
rm -rf classes/*
echo Compiling $EXAMPLE ...
javac -cp $JAVA_CP -d classes $EXAMPLE.java
JAVAC_RET=$?
if [ $JAVAC_RET -ne 0 ]; then
  echo "Error: javac failed with exit code $JAVAC_RET"
  exit $JAVAC_RET
fi

echo Running $EXAMPLE...
JAVA_OPTS="-server -Xmx1g -Xms1g"
java ${JAVA_OPTS} -cp $JAVA_CP:classes $EXAMPLE $NUM_NODES
