# Simple Cassovary server

A simple Http server using Cassovary that opens up two ports -- localhost:8888 for doing
a dummy graph operation and and localhost:9999 for doing admin and stats reporting.

## Building
In the main project folder at the top of git tree, type `./sbt 'project cassovary-server' run'.
In a separate window, hit the service by trying 'curl localhost:8888'. Then look at some stats
maintained by the [Ostrich](https://github.com/twitter/ostrich) library used in Cassovary
by trying 'curl localhost:9999/stats.txt'.
