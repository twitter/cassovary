#!/bin/bash

root=$(
  cd $(dirname $(readlink $0 || echo $0))/..
  /bin/pwd
)

sbtjar=sbt-launch.jar
sbtver=0.13.9
SBTURL=http://dl.bintray.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/$sbtver/$sbtjar
JARMD5=767d963ed266459aa8bf32184599786d

if [ ! -f $sbtjar ]; then
  echo "downloading $sbtjar" 1>&2
  curl --location --remote-name $SBTURL
  echo "copying to $HOME/.sbt/launchers/$sbtver/$sbtjar"
  cp $sbtjar $HOME/.sbt/launchers/$sbtver/
fi

test -f $sbtjar || exit 1
sbtjar_md5=$(openssl md5 < $sbtjar|cut -f2 -d'='|awk '{print $1}')
if [ "${sbtjar_md5}" != "$JARMD5" ]; then
  echo "Expected checksum $JARMD5, but got ${sbtjar_md5}"
  echo "bad $sbtjar.  delete $sbtjar and run $0 again."
  exit 1
fi


test -f ~/.sbtconfig && . ~/.sbtconfig

java -ea                          \
  $SBT_OPTS                       \
  $JAVA_OPTS                      \
  -Djava.net.preferIPv4Stack=true \
  -XX:+AggressiveOpts             \
  -XX:+UseParNewGC                \
  -XX:+UseConcMarkSweepGC         \
  -XX:+CMSParallelRemarkEnabled   \
  -XX:+CMSClassUnloadingEnabled   \
  -XX:PermSize=256m               \
  -XX:SurvivorRatio=128           \
  -XX:MaxTenuringThreshold=0      \
  -XX:ReservedCodeCacheSize=128m  \
  -Xss8M                          \
  -Xms512M                        \
  -Xmx1G                          \
  -server                         \
  -jar $sbtjar "$@"
