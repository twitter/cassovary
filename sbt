#!/bin/bash
java -Xmx1g -XX:MaxPermSize=256m -jar project/sbt-launch.jar "$@"
