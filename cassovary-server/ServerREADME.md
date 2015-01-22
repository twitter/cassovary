# Simple Cassovary server

A simple Http server using Cassovary that opens up two ports -- localhost:8888 for doing
a dummy graph operation and and localhost:9990 for doing admin and stats reporting.

## Building
In the main project folder at the top of git tree, type `./sbt 'project cassovary-server' run'.
In a separate window, hit the service by trying 'curl localhost:8888'. Then look at some stats
maintained by the [Finagle-Stats](https://github.com/twitter/finagle/tree/master/finagle-stats) library used in Cassovary
by trying 'curl localhost:9990/admin/metrics.json'.
 
## Filtering stats out
By passing a comma-separated list of regexes to exclude from stats using `-com.twitter.finagle.stats.statsFilter` flag,
one can single out the stats that will not be shown when queried with `filtered=true`. 
For example, to filter out all stats starting with jvm and also any p90 stats, one can pass the following to 
Cassovary Server: `-com.twitter.finagle.stats.statsFilter="jvm.*,.*\.p90"`
To query the reduced list: `curl localhost:9990/admin/metrics.json?filtered=true`
