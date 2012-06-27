# Code examples

## Building a tiny example using Scala
See the included script in the ```scala``` folder, ```build_run_example.sh``` as an
example of how to build the library and use it with one toy example, HelloGraph.scala.
This example simply creates a complete directed graph with 100 nodes and prints the number
of edges in this graph. You can run this script by just typing ```examples/scala/build_run_example.sh```
which will build and run the HelloGraph.scala example with default options. You can also run
other examples for instance by running ```examples/scala/build_run_example.sh RandomWalk 10000```

## Building a tiny example using Java
The ```java``` directory contains the same examples, but written in Java. See ```build_run_example.sh```
in that directory to see how to build HelloGraph.java. As above, you can also run other examples like
```examples/java/build_run_example.sh RandomWalk 10000```.

* Note - as cassovary was designed primarily for Scala users, the Java examples are considerably
more verbose. Improvements on Java interfaces are a work in progress.