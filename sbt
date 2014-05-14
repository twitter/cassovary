#!/bin/bash

root=$(
  cd $(dirname $(readlink $0 || echo $0))/..
  /bin/pwd
)

sbtjar=sbt-launch.jar
sbtver=0.13.2

if [ ! -f $sbtjar ]; then
  echo "downloading $sbtjar" 1>&2
  curl -O http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/$sbtver/$sbtjar
fi

test -f $sbtjar || exit 1
sbtjar_md5=$(openssl md5 < $sbtjar|cut -f2 -d'='|awk '{print $1}')
if [ "${sbtjar_md5}" != 3bc22e5885fa0792ff500a8e91d0936d ]; then
  echo 'bad sbtjar!' 1>&2
  exit 1
fi


test -f ~/.sbtconfig && . ~/.sbtconfig

# Removing -ea here because of https://groups.google.com/forum/#!topic/scala-language/FJi8n2dfD3k
# Running into this issue with FileChannel.MapMode
java                              \
  $SBT_OPTS                       \
  $JAVA_OPTS                      \
  -Djava.net.preferIPv4Stack=true \
  -XX:+AggressiveOpts             \
  -XX:+UseParNewGC                \
  -XX:+UseConcMarkSweepGC         \
  -XX:+CMSParallelRemarkEnabled   \
  -XX:+CMSClassUnloadingEnabled   \
  -XX:MaxPermSize=1024m           \
  -XX:SurvivorRatio=128           \
  -XX:MaxTenuringThreshold=0      \
  -XX:ReservedCodeCacheSize=128m  \
  -Xss8M                          \
  -Xms512M                        \
  -Xmx1G                          \
  -server                         \
  -jar $sbtjar "$@"
