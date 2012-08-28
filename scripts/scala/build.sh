#!/bin/sh

BUILD_PACKAGE="build"

if [ -z "$1" ]; then
  EXAMPLE=RenumbererMapper
else
  EXAMPLE="$1"
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
  sbt update
  echo Building Cassovary jar...
  sbt package
fi

JAVA_CP=(
  $(find $root/target -name 'cassovary*.jar') \
  $HOME/.sbt/boot/scala-2.8.1/lib/scala-library.jar \
  $(find $root/lib_managed/jars/ -name '*.jar')
)

JAVA_CP=$(echo ${JAVA_CP[@]} | tr ' ' ':')

cd scripts/scala
mkdir -p classes
rm -rf classes/*
echo Compiling $EXAMPLE ...
scalac -cp $JAVA_CP -d classes $EXAMPLE.scala

echo Running $EXAMPLE...
JAVA_OPTS="-server -Xmx14g -Xms14g"
java ${JAVA_OPTS} -cp $JAVA_CP:classes $EXAMPLE "$@"
